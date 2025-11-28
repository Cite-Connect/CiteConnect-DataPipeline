-- ============================================================================
-- CiteConnect Complete Database Schema
-- Version: 2.0 - Production Ready
-- Date: November 25, 2024
-- Purpose: Complete schema replacement with ground truth and dual embeddings
-- ============================================================================

-- WARNING: This script drops ALL existing CiteConnect tables
-- Ensure you have backups if needed before running

-- ============================================================================
-- STEP 1: DROP ALL EXISTING TABLES (Clean Slate)
-- ============================================================================

DROP TABLE IF EXISTS public.recommendation_cache CASCADE;
DROP TABLE IF EXISTS public.user_paper_filters CASCADE;
DROP TABLE IF EXISTS public.user_interactions CASCADE;
DROP TABLE IF EXISTS public.recommendation_events CASCADE;
DROP TABLE IF EXISTS public.ab_test_comparisons CASCADE;
DROP TABLE IF EXISTS public.experiment_runs CASCADE;
DROP TABLE IF EXISTS public.interaction_evaluations CASCADE;
DROP TABLE IF EXISTS public.cold_start_evaluations CASCADE;
DROP TABLE IF EXISTS public.domain_canonical_papers CASCADE;
DROP TABLE IF EXISTS public.ground_truth_relationships CASCADE;
DROP TABLE IF EXISTS public.ground_truth_papers CASCADE;
DROP TABLE IF EXISTS public.user_recommendation_state CASCADE;
DROP TABLE IF EXISTS public.user_embeddings_specter CASCADE;
DROP TABLE IF EXISTS public.user_embeddings_minilm CASCADE;
DROP TABLE IF EXISTS public.paper_embeddings_specter CASCADE;
DROP TABLE IF EXISTS public.paper_embeddings_minilm CASCADE;
DROP TABLE IF EXISTS public.user_interest_hierarchy CASCADE;
DROP TABLE IF EXISTS public.user_profiles_extended CASCADE;
DROP TABLE IF EXISTS public.user_profile_embeddings CASCADE;
DROP TABLE IF EXISTS public.paper_clusters CASCADE;
DROP TABLE IF EXISTS public.cluster_papers CASCADE;
DROP TABLE IF EXISTS public.user_saved_papers CASCADE;
DROP TABLE IF EXISTS public.user_liked_papers CASCADE;
DROP TABLE IF EXISTS public.user_interests CASCADE;
DROP TABLE IF EXISTS public.user_domains CASCADE;
DROP TABLE IF EXISTS public.papers CASCADE;
DROP TABLE IF EXISTS public.users CASCADE;

-- Drop old sequences if they exist
DROP SEQUENCE IF EXISTS users_user_id_seq CASCADE;

-- ============================================================================
-- STEP 2: ENABLE REQUIRED EXTENSIONS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- For fuzzy text search
CREATE EXTENSION IF NOT EXISTS btree_gin; -- For composite GIN indexes

-- ============================================================================
-- STEP 3: CORE USER TABLES
-- ============================================================================

CREATE TABLE public.users (
  user_id SERIAL PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  is_active BOOLEAN NOT NULL DEFAULT true
);

COMMENT ON TABLE public.users IS 
'Core user authentication and identification table';

-- Enhanced user profile for rich cold-start recommendations
CREATE TABLE public.user_profiles_extended (
  user_id INTEGER PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
  
  -- Research Profile (collected at registration)
  research_stage VARCHAR(50) CHECK (research_stage IN (
    'undergraduate', 'masters', 'phd', 'postdoc', 'professor', 'industry', 'independent'
  )),
  years_experience INTEGER CHECK (years_experience >= 0 AND years_experience <= 50),
  h_index INTEGER CHECK (h_index >= 0),
  
  -- Primary domain (user selects ONE at registration)
  primary_domain VARCHAR(50) NOT NULL CHECK (primary_domain IN (
    'healthcare', 'fintech', 'quantum_computing'
  )),
  
  -- Sub-domains (search terms user explores - multiple allowed)
  sub_domains TEXT[], -- ['computer_vision', 'medical_imaging', 'LLM', 'transformers']
  research_methods TEXT[], -- ['deep_learning', 'statistical_analysis', 'clinical_trials']
  
  -- Goals and Intent (drives recommendation strategy)
  research_goals TEXT[] CHECK (array_length(research_goals, 1) <= 5), 
  -- ['publish_paper', 'literature_review', 'find_collaborators', 'stay_updated', 'learn_new_topic']
  
  reading_level VARCHAR(50) NOT NULL CHECK (reading_level IN (
    'introductory', 'intermediate', 'advanced', 'expert'
  )),
  
  time_availability VARCHAR(50) CHECK (time_availability IN (
    'casual_reader', 'part_time_researcher', 'full_time_researcher'
  )),
  
  -- Preferences (affects recommendation filtering)
  prefers_recent_papers BOOLEAN DEFAULT true,
  prefers_high_impact BOOLEAN DEFAULT false,
  prefers_open_access BOOLEAN DEFAULT true,
  preferred_venues TEXT[], -- ['Nature', 'ICML', 'NeurIPS', 'Cell']
  preferred_publication_years INT4RANGE, -- e.g., '[2020,2024]'
  
  -- Institution (optional)
  institution VARCHAR(200),
  department VARCHAR(200),
  looking_for_collaborators BOOLEAN DEFAULT false,
  
  -- Google Scholar integration (optional)
  google_scholar_url VARCHAR(500),
  semantic_scholar_author_id VARCHAR(100),
  
  -- Profile quality (auto-calculated)
  profile_completeness FLOAT GENERATED ALWAYS AS (
    (CASE WHEN research_stage IS NOT NULL THEN 0.2 ELSE 0 END) +
    (CASE WHEN primary_domain IS NOT NULL THEN 0.2 ELSE 0 END) +
    (CASE WHEN array_length(sub_domains, 1) > 0 THEN 0.2 ELSE 0 END) +
    (CASE WHEN array_length(research_goals, 1) > 0 THEN 0.2 ELSE 0 END) +
    (CASE WHEN reading_level IS NOT NULL THEN 0.2 ELSE 0 END)
  ) STORED,
  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.user_profiles_extended IS 
'Extended user profiles for rich cold-start recommendations - CiteConnect USP';

-- Hierarchical interest tracking (3 levels: broad → specific → narrow)
CREATE TABLE public.user_interest_hierarchy (
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  interest_level INTEGER NOT NULL CHECK (interest_level IN (1, 2, 3)), 
  -- 1=broad ("AI"), 2=specific ("Computer Vision"), 3=narrow ("Medical Segmentation")
  interest_term VARCHAR(100) NOT NULL,
  confidence_score FLOAT DEFAULT 1.0 CHECK (confidence_score >= 0 AND confidence_score <= 1),
  source VARCHAR(50) NOT NULL CHECK (source IN ('explicit', 'inferred', 'imported')),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (user_id, interest_level, interest_term)
);

COMMENT ON TABLE public.user_interest_hierarchy IS 
'Multi-level interest tracking from broad to specific topics';

-- ============================================================================
-- STEP 4: PAPER TABLES WITH CITATION NETWORKS
-- ============================================================================

CREATE TABLE public.papers (
  paper_id VARCHAR(100) PRIMARY KEY,
  
  -- Core metadata
  title TEXT NOT NULL,
  authors TEXT[],
  year INTEGER CHECK (year >= 1900 AND year <= 2100),
  venue VARCHAR(255),
  citation_count INTEGER NOT NULL DEFAULT 0 CHECK (citation_count >= 0),
  
  -- Content fields
  abstract TEXT,
  introduction TEXT,
  tldr TEXT, -- Quick summary for UI cards
  
  -- Domain classification
  domain VARCHAR(50) CHECK (domain IN (
    'healthcare', 'fintech', 'quantum_computing'
  ) OR domain IS NULL),
  domain_confidence FLOAT DEFAULT 0.0 CHECK (domain_confidence >= 0 AND domain_confidence <= 1),
  
  -- Sub-domain tags (from search terms)
  sub_domains TEXT[], -- ['LLM', 'computer_vision', 'transformers', 'medical_imaging']
  
  -- Citation network arrays (populated by teammate's pipeline)
  reference_ids TEXT[], -- Papers this paper cites (renamed from 'references' - reserved keyword)
  citation_ids TEXT[],  -- Papers citing this paper (renamed from 'citations' for consistency)
  reference_count INTEGER DEFAULT 0, -- For quick queries
  
  -- Quality indicators
  quality_score FLOAT DEFAULT 0.0 CHECK (quality_score >= 0 AND quality_score <= 1),
  extraction_method VARCHAR(50) CHECK (extraction_method IN (
    'arxiv_html', 'grobid_pdf', 'regex_pdf', 'abstract_only', 'abstract_tldr', NULL
  )),
  content_quality VARCHAR(20) CHECK (content_quality IN ('high', 'medium', 'low', NULL)),
  has_introduction BOOLEAN DEFAULT false,
  intro_length INTEGER DEFAULT 0 CHECK (intro_length >= 0),
  
  -- Storage
  gcs_pdf_path VARCHAR(500),
  
  -- Timestamps
  ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.papers IS 
'Central repository of academic papers with full citation network';

COMMENT ON COLUMN public.papers.references IS 
'Array of paper_ids this paper cites (bibliography) - used for ground truth evaluation';

COMMENT ON COLUMN public.papers.citations IS 
'Array of paper_ids citing this paper (forward citations) - impact metric';

-- ============================================================================
-- STEP 5: DUAL EMBEDDING TABLES (384-dim + 768-dim)
-- ============================================================================

-- Paper embeddings: all-MiniLM-L6-v2 (384 dimensions)
CREATE TABLE public.paper_embeddings_minilm (
  paper_id VARCHAR(100) PRIMARY KEY REFERENCES papers(paper_id) ON DELETE CASCADE,
  
  -- Vector embedding
  embedding vector(384) NOT NULL,
  
  -- Model metadata
  model_name VARCHAR(100) NOT NULL DEFAULT 'sentence-transformers/all-MiniLM-L6-v2',
  model_version VARCHAR(50) NOT NULL DEFAULT 'v1.0',
  
  -- Source tracking (for reproducibility)
  embedding_source TEXT, -- "title + abstract + introduction (2487 chars)"
  source_hash VARCHAR(64), -- SHA-256 hash
  text_length INTEGER,
  has_introduction BOOLEAN DEFAULT false,
  
  -- Timestamps
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.paper_embeddings_minilm IS 
'384-dimensional embeddings from all-MiniLM-L6-v2 - optimized for speed';

-- Paper embeddings: allenai/specter (768 dimensions)
CREATE TABLE public.paper_embeddings_specter (
  paper_id VARCHAR(100) PRIMARY KEY REFERENCES papers(paper_id) ON DELETE CASCADE,
  
  -- Vector embedding
  embedding vector(768) NOT NULL,
  
  -- Model metadata
  model_name VARCHAR(100) NOT NULL DEFAULT 'allenai/specter',
  model_version VARCHAR(50) NOT NULL DEFAULT 'v1.0',
  
  -- Source tracking
  embedding_source TEXT,
  source_hash VARCHAR(64),
  text_length INTEGER,
  has_introduction BOOLEAN DEFAULT false,
  
  -- Timestamps
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.paper_embeddings_specter IS 
'768-dimensional embeddings from allenai/specter - trained on 145M citation relationships';

-- User embeddings: all-MiniLM (384 dimensions)
CREATE TABLE public.user_embeddings_minilm (
  user_id INTEGER PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
  
  embedding vector(384) NOT NULL,
  
  -- Generation method
  generation_method VARCHAR(50) NOT NULL CHECK (generation_method IN (
    'profile_based',      -- From user profile (cold start)
    'interaction_based',  -- From saved/liked papers
    'hybrid'             -- Combination of both
  )),
  
  -- Metadata
  based_on_papers TEXT[], -- Paper IDs used to generate this
  interaction_count INTEGER DEFAULT 0,
  
  -- Timestamps
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.user_embeddings_minilm IS 
'User profile embeddings (384-dim) for personalized search with mini-LM';

-- User embeddings: SPECTER (768 dimensions)
CREATE TABLE public.user_embeddings_specter (
  user_id INTEGER PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
  
  embedding vector(768) NOT NULL,
  
  generation_method VARCHAR(50) NOT NULL CHECK (generation_method IN (
    'profile_based', 'interaction_based', 'hybrid'
  )),
  
  based_on_papers TEXT[],
  interaction_count INTEGER DEFAULT 0,
  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.user_embeddings_specter IS 
'User profile embeddings (768-dim) for personalized search with SPECTER';

-- ============================================================================
-- STEP 6: GROUND TRUTH TABLES (For Evaluation)
-- ============================================================================

CREATE TABLE public.ground_truth_papers (
  paper_id VARCHAR(100) PRIMARY KEY REFERENCES papers(paper_id) ON DELETE CASCADE,
  
  -- Selection criteria (papers with good citation networks)
  reference_count INTEGER NOT NULL CHECK (reference_count BETWEEN 10 AND 100),
  references_in_corpus INTEGER NOT NULL, -- How many refs we have
  reference_coverage FLOAT GENERATED ALWAYS AS (
    CASE 
      WHEN reference_count > 0 THEN references_in_corpus::FLOAT / reference_count 
      ELSE 0 
    END
  ) STORED,
  
  -- Quality metrics
  quality_score FLOAT CHECK (quality_score >= 0 AND quality_score <= 1),
  citation_score FLOAT, -- Normalized citation impact
  recency_score FLOAT,  -- Recency factor
  completeness_score FLOAT, -- Has abstract, intro, good extraction
  
  -- Classification
  domain VARCHAR(50) NOT NULL CHECK (domain IN (
    'healthcare', 'fintech', 'quantum_computing'
  )),
  
  -- Canonical status (for cold start recommendations)
  is_canonical BOOLEAN DEFAULT false,
  canonical_tier VARCHAR(20) CHECK (canonical_tier IN (
    'foundational', 'trending', 'recent', NULL
  )),
  domain_rank INTEGER, -- Rank within domain
  
  established_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  -- Only papers with good coverage qualify
  CHECK (reference_coverage >= 0.3)
);

COMMENT ON TABLE public.ground_truth_papers IS 
'High-quality papers with citation networks for objective evaluation';

CREATE TABLE public.ground_truth_relationships (
  paper_id VARCHAR(100) PRIMARY KEY REFERENCES ground_truth_papers(paper_id) ON DELETE CASCADE,
  
  -- Citation network (direct relationships)
  citation_network TEXT[] NOT NULL, -- Combined references + citations
  citation_network_size INTEGER,
  
  -- Co-citation relationships (papers cited together)
  co_cited_papers TEXT[],
  co_citation_strengths FLOAT[], -- Frequency scores
  
  -- Bibliographic coupling (papers citing same sources)
  bibliographic_couples TEXT[],
  coupling_strengths FLOAT[],
  
  -- Network metrics
  relationship_quality_score FLOAT,
  network_centrality FLOAT, -- PageRank or betweenness
  
  computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.ground_truth_relationships IS 
'Pre-computed citation relationships for fast evaluation and recommendations';

-- Canonical papers per domain (for cold start)
CREATE TABLE public.domain_canonical_papers (
  domain VARCHAR(50) NOT NULL CHECK (domain IN (
    'healthcare', 'fintech', 'quantum_computing'
  )),
  recommendation_tier VARCHAR(20) NOT NULL CHECK (recommendation_tier IN (
    'foundational', 'trending', 'recent'
  )),
  
  paper_ids TEXT[] NOT NULL,
  avg_quality_score FLOAT,
  paper_count INTEGER GENERATED ALWAYS AS (array_length(paper_ids, 1)) STORED,
  
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (domain, recommendation_tier)
);

COMMENT ON TABLE public.domain_canonical_papers IS 
'Curated high-quality papers for each domain tier - used for cold start recommendations';

-- ============================================================================
-- STEP 7: EVALUATION TABLES (Simplified 2-Metric Approach)
-- ============================================================================

CREATE TABLE public.cold_start_evaluations (
  evaluation_id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  embedding_model VARCHAR(50) NOT NULL,
  
  -- Our 2 core metrics for cold start
  profile_alignment FLOAT NOT NULL CHECK (profile_alignment >= 0 AND profile_alignment <= 1),
  ground_truth_quality FLOAT NOT NULL CHECK (ground_truth_quality >= 0 AND ground_truth_quality <= 1),
  
  -- Combined score (60% alignment + 40% quality)
  combined_score FLOAT GENERATED ALWAYS AS (
    profile_alignment * 0.6 + ground_truth_quality * 0.4
  ) STORED,
  
  -- Metadata
  recommendation_count INTEGER,
  evaluation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.cold_start_evaluations IS 
'Simplified 2-metric evaluation for cold start recommendations';

CREATE TABLE public.interaction_evaluations (
  evaluation_id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  embedding_model VARCHAR(50) NOT NULL,
  
  -- Core metrics (carried forward)
  profile_alignment FLOAT,
  ground_truth_quality FLOAT,
  
  -- Real user behavior metrics
  click_through_rate FLOAT CHECK (click_through_rate >= 0 AND click_through_rate <= 1),
  save_rate FLOAT CHECK (save_rate >= 0 AND save_rate <= 1),
  precision_at_10 FLOAT CHECK (precision_at_10 >= 0 AND precision_at_10 <= 1),
  recall_at_10 FLOAT CHECK (recall_at_10 >= 0 AND recall_at_10 <= 1),
  
  -- Evaluation context
  interaction_count INTEGER NOT NULL,
  evaluation_window_hours INTEGER DEFAULT 168, -- 1 week
  evaluation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.interaction_evaluations IS 
'Enhanced evaluation metrics after users start interacting';

-- ============================================================================
-- STEP 8: EXPERIMENT TRACKING (MLOps)
-- ============================================================================

CREATE TABLE public.experiment_runs (
  run_id VARCHAR(100) PRIMARY KEY,
  
  -- Model configuration
  embedding_model VARCHAR(50) NOT NULL,
  embedding_dimension INTEGER NOT NULL CHECK (embedding_dimension IN (384, 768)),
  
  -- Recommendation hyperparameters
  hyperparameters JSONB NOT NULL DEFAULT '{
    "semantic_weight": 0.35,
    "citation_weight": 0.20,
    "keyword_weight": 0.15,
    "popularity_weight": 0.15,
    "recency_weight": 0.10,
    "diversity_weight": 0.05
  }'::jsonb,
  
  -- Experiment metadata
  experiment_type VARCHAR(50) NOT NULL CHECK (experiment_type IN (
    'baseline', 'cold_start_test', 'a_b_test', 'shadow_mode', 'champion_challenger'
  )),
  user_segment VARCHAR(50) CHECK (user_segment IN (
    'cold_start', 'early', 'mature', 'expert', 'all'
  )),
  
  -- Status tracking
  status VARCHAR(20) DEFAULT 'running' CHECK (status IN (
    'running', 'completed', 'failed', 'cancelled'
  )),
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ended_at TIMESTAMP,
  
  -- MLflow integration
  mlflow_experiment_id VARCHAR(100),
  mlflow_run_id VARCHAR(100),
  
  -- Results summary (filled when complete)
  results JSONB
);

COMMENT ON TABLE public.experiment_runs IS 
'Track ML experiments for model comparison and optimization';

CREATE TABLE public.ab_test_comparisons (
  test_id SERIAL PRIMARY KEY,
  test_name VARCHAR(100) NOT NULL,
  
  -- Models being compared
  model_a VARCHAR(50) NOT NULL,
  model_a_run_id VARCHAR(100) REFERENCES experiment_runs(run_id) ON DELETE CASCADE,
  
  model_b VARCHAR(50) NOT NULL,
  model_b_run_id VARCHAR(100) REFERENCES experiment_runs(run_id) ON DELETE CASCADE,
  
  -- Test configuration
  traffic_split FLOAT DEFAULT 0.5 CHECK (traffic_split > 0 AND traffic_split < 1),
  min_sample_size INTEGER DEFAULT 100,
  
  -- Results (statistical significance)
  winner VARCHAR(50),
  confidence_level FLOAT CHECK (confidence_level >= 0 AND confidence_level <= 1),
  p_value FLOAT,
  effect_size FLOAT,
  
  -- Timing
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ended_at TIMESTAMP,
  status VARCHAR(20) DEFAULT 'running' CHECK (status IN (
    'running', 'completed', 'inconclusive', 'cancelled'
  ))
);

COMMENT ON TABLE public.ab_test_comparisons IS 
'A/B testing framework for comparing embedding models with statistical rigor';

-- ============================================================================
-- STEP 9: USER ACTIVITY & INTERACTION TRACKING
-- ============================================================================

CREATE TABLE public.user_recommendation_state (
  user_id INTEGER PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
  
  -- Journey stage
  recommendation_stage VARCHAR(20) DEFAULT 'cold_start' CHECK (
    recommendation_stage IN ('cold_start', 'early', 'mature', 'expert')
  ),
  interaction_count INTEGER DEFAULT 0 CHECK (interaction_count >= 0),
  
  -- Profile versioning
  profile_version INTEGER DEFAULT 1,
  
  -- Model update tracking
  last_embedding_update_minilm TIMESTAMP,
  last_embedding_update_specter TIMESTAMP,
  
  -- Model preference (if discovered through A/B testing)
  preferred_model VARCHAR(50),
  model_preference_confidence FLOAT CHECK (
    model_preference_confidence >= 0 AND model_preference_confidence <= 1
  ),
  
  -- Activity timestamps
  last_login TIMESTAMP,
  last_recommendation_generated TIMESTAMP,
  
  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.user_recommendation_state IS 
'Track user journey stage and determine recommendation strategy';

CREATE TABLE public.recommendation_events (
  event_id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  
  -- Model used for this recommendation
  embedding_model VARCHAR(50) NOT NULL,
  run_id VARCHAR(100) REFERENCES experiment_runs(run_id),
  
  -- What was recommended
  recommended_paper_ids TEXT[] NOT NULL,
  recommendation_scores FLOAT[],
  
  -- Generation strategy
  recommendation_strategy VARCHAR(50) NOT NULL CHECK (recommendation_strategy IN (
    'cold_start_profile',
    'cold_start_canonical',
    'interaction_based',
    'citation_network',
    'hybrid'
  )),
  
  -- User response (updated as they interact)
  clicked_paper_ids TEXT[],
  saved_paper_ids TEXT[],
  liked_paper_ids TEXT[],
  dwell_times JSONB, -- {"paper_id": seconds}
  
  -- Context
  user_stage VARCHAR(20),
  session_id VARCHAR(100),
  
  event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  -- Constraint: scores match paper count
  CHECK (array_length(recommended_paper_ids, 1) = array_length(recommendation_scores, 1))
);

COMMENT ON TABLE public.recommendation_events IS 
'Detailed event log linking recommendations to user responses';

CREATE TABLE public.user_interactions (
  interaction_id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  paper_id VARCHAR(100) NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
  
  -- Interaction type and strength
  interaction_type VARCHAR(50) NOT NULL CHECK (interaction_type IN (
    'view', 'click', 'save', 'like', 'download', 
    'cite', 'dismiss', 'not_interested', 'read_time'
  )),
  
  -- Auto-calculated interaction strength
  interaction_strength FLOAT GENERATED ALWAYS AS (
    CASE interaction_type
      WHEN 'cite' THEN 1.0
      WHEN 'save' THEN 0.8
      WHEN 'download' THEN 0.7
      WHEN 'like' THEN 0.6
      WHEN 'click' THEN 0.3
      WHEN 'view' THEN 0.2
      WHEN 'dismiss' THEN -0.2
      WHEN 'not_interested' THEN -0.5
      ELSE 0.1
    END
  ) STORED,
  
  duration_seconds INTEGER,
  
  -- Attribution (which model recommendation led here)
  source_embedding_model VARCHAR(50),
  source_run_id VARCHAR(100),
  source_recommendation_event_id INTEGER,
  
  -- Context
  context JSONB, -- Flexible additional data
  
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  -- Index for fast user lookups
  CONSTRAINT idx_user_paper_interaction UNIQUE (user_id, paper_id, interaction_type, created_at)
);

COMMENT ON TABLE public.user_interactions IS 
'Complete interaction history with calculated strength scores for personalization';

CREATE TABLE public.user_saved_papers (
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  paper_id VARCHAR(100) REFERENCES papers(paper_id) ON DELETE CASCADE,
  saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  notes TEXT,
  
  PRIMARY KEY (user_id, paper_id)
);

CREATE TABLE public.user_liked_papers (
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  paper_id VARCHAR(100) REFERENCES papers(paper_id) ON DELETE CASCADE,
  liked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (user_id, paper_id)
);

CREATE TABLE public.user_paper_filters (
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  paper_id VARCHAR(100) REFERENCES papers(paper_id) ON DELETE CASCADE,
  filter_type VARCHAR(20) NOT NULL CHECK (filter_type IN (
    'not_interested', 'already_read', 'saved', 'dismissed'
  )),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  PRIMARY KEY (user_id, paper_id)
);

COMMENT ON TABLE public.user_paper_filters IS 
'Papers to exclude from future recommendations';

-- ============================================================================
-- STEP 10: PERFORMANCE & CACHING
-- ============================================================================

CREATE TABLE public.recommendation_cache (
  cache_id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
  embedding_model VARCHAR(50) NOT NULL,
  
  -- Cached recommendations
  paper_ids TEXT[] NOT NULL,
  scores FLOAT[] NOT NULL,
  
  -- Cache metadata
  generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP NOT NULL,
  is_consumed BOOLEAN DEFAULT FALSE,
  
  -- Generation context
  user_stage VARCHAR(20),
  strategy_used VARCHAR(50),
  generation_time_ms INTEGER,
  
  CHECK (array_length(paper_ids, 1) = array_length(scores, 1))
);

COMMENT ON TABLE public.recommendation_cache IS 
'Pre-computed recommendations for fast serving';

-- ============================================================================
-- STEP 11: CRITICAL INDEXES (Performance)
-- ============================================================================

-- Vector similarity indexes (IVFFlat for approximate nearest neighbor)
CREATE INDEX idx_paper_embeddings_minilm_vector 
ON paper_embeddings_minilm 
USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 100);

CREATE INDEX idx_paper_embeddings_specter_vector 
ON paper_embeddings_specter 
USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 100);

CREATE INDEX idx_user_embeddings_minilm_vector 
ON user_embeddings_minilm 
USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 50);

CREATE INDEX idx_user_embeddings_specter_vector 
ON user_embeddings_specter 
USING ivfflat (embedding vector_cosine_ops) 
WITH (lists = 50);

-- Paper metadata indexes
CREATE INDEX idx_papers_domain ON papers(domain) WHERE domain IS NOT NULL;
CREATE INDEX idx_papers_year ON papers(year DESC) WHERE year IS NOT NULL;
CREATE INDEX idx_papers_citation_count ON papers(citation_count DESC);
CREATE INDEX idx_papers_quality_score ON papers(quality_score DESC) WHERE quality_score IS NOT NULL;
CREATE INDEX idx_papers_sub_domains ON papers USING gin(sub_domains);

-- Full-text search indexes
CREATE INDEX idx_papers_title_gin 
ON papers USING gin(to_tsvector('english', title));

CREATE INDEX idx_papers_abstract_gin 
ON papers USING gin(to_tsvector('english', abstract)) 
WHERE abstract IS NOT NULL;

-- Ground truth indexes
CREATE INDEX idx_ground_truth_papers_domain 
ON ground_truth_papers(domain);

CREATE INDEX idx_ground_truth_papers_canonical 
ON ground_truth_papers(is_canonical) 
WHERE is_canonical = true;

CREATE INDEX idx_ground_truth_papers_tier 
ON ground_truth_papers(domain, canonical_tier) 
WHERE canonical_tier IS NOT NULL;

CREATE INDEX idx_ground_truth_relationships_network 
ON ground_truth_relationships USING gin(citation_network);

-- User activity indexes
CREATE INDEX idx_user_interactions_user 
ON user_interactions(user_id, created_at DESC);

CREATE INDEX idx_user_interactions_paper 
ON user_interactions(paper_id);

CREATE INDEX idx_user_interactions_type 
ON user_interactions(user_id, interaction_type);

CREATE INDEX idx_user_saved_papers_user 
ON user_saved_papers(user_id, saved_at DESC);

CREATE INDEX idx_recommendation_events_user 
ON recommendation_events(user_id, event_timestamp DESC);

CREATE INDEX idx_recommendation_events_model 
ON recommendation_events(embedding_model);

-- Evaluation indexes
CREATE INDEX idx_cold_start_eval_user_model 
ON cold_start_evaluations(user_id, embedding_model);

CREATE INDEX idx_cold_start_eval_score 
ON cold_start_evaluations(combined_score DESC);

-- Cache indexes
CREATE INDEX idx_recommendation_cache_user 
ON recommendation_cache(user_id, expires_at) 
WHERE is_consumed = false;

-- ============================================================================
-- STEP 12: TRIGGERS FOR AUTO-UPDATE
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to relevant tables
CREATE TRIGGER update_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_profiles_updated_at
    BEFORE UPDATE ON user_profiles_extended
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_papers_updated_at
    BEFORE UPDATE ON papers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_paper_embeddings_minilm_updated_at
    BEFORE UPDATE ON paper_embeddings_minilm
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_paper_embeddings_specter_updated_at
    BEFORE UPDATE ON paper_embeddings_specter
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_recommendation_state_updated_at
    BEFORE UPDATE ON user_recommendation_state
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- STEP 13: VERIFICATION QUERIES
-- ============================================================================

-- Check all tables created
DO $$
DECLARE
    expected_tables TEXT[] := ARRAY[
        'users', 'user_profiles_extended', 'user_interest_hierarchy',
        'papers', 'paper_embeddings_minilm', 'paper_embeddings_specter',
        'user_embeddings_minilm', 'user_embeddings_specter',
        'ground_truth_papers', 'ground_truth_relationships', 'domain_canonical_papers',
        'cold_start_evaluations', 'interaction_evaluations',
        'experiment_runs', 'ab_test_comparisons',
        'user_recommendation_state', 'recommendation_events',
        'user_interactions', 'user_saved_papers', 'user_liked_papers',
        'user_paper_filters', 'recommendation_cache'
    ];
    actual_count INTEGER;
    table_name TEXT;
BEGIN
    FOR table_name IN SELECT unnest(expected_tables) LOOP
        SELECT COUNT(*) INTO actual_count
        FROM information_schema.tables
        WHERE table_schema = 'public' AND tables.table_name = table_name;
        
        IF actual_count = 0 THEN
            RAISE EXCEPTION 'Table % not found!', table_name;
        END IF;
    END LOOP;
    
    RAISE NOTICE '✅ All % tables created successfully', array_length(expected_tables, 1);
END $$;

-- Check vector indexes
DO $$
DECLARE
    vector_index_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO vector_index_count
    FROM pg_indexes
    WHERE indexname LIKE '%_vector';
    
    IF vector_index_count < 4 THEN
        RAISE EXCEPTION 'Expected at least 4 vector indexes, found %', vector_index_count;
    END IF;
    
    RAISE NOTICE '✅ Vector indexes created: %', vector_index_count;
END $$;

-- Display table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    0 as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════';
    RAISE NOTICE '✅ CiteConnect Schema v2.0 Created Successfully';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════';
    RAISE NOTICE '';
    RAISE NOTICE 'Created Table Groups:';
    RAISE NOTICE '  📋 User Management: 3 tables';
    RAISE NOTICE '  📄 Papers & Content: 1 table';
    RAISE NOTICE '  🧬 Embeddings (Dual): 4 tables (384-dim + 768-dim)';
    RAISE NOTICE '  🎯 Ground Truth: 3 tables';
    RAISE NOTICE '  📊 Evaluation: 2 tables';
    RAISE NOTICE '  🔬 Experiments: 2 tables';
    RAISE NOTICE '  👤 User Activity: 6 tables';
    RAISE NOTICE '  ⚡ Performance: 1 table';
    RAISE NOTICE '';
    RAISE NOTICE 'Total: 22 tables created';
    RAISE NOTICE '';
    RAISE NOTICE 'Vector Indexes: 4 (IVFFlat for fast similarity search)';
    RAISE NOTICE 'Regular Indexes: 20+ (Optimized for queries)';
    RAISE NOTICE '';
    RAISE NOTICE 'Next Steps:';
    RAISE NOTICE '  1. Update data pipeline to populate papers + embeddings';
    RAISE NOTICE '  2. Create seed users with extended profiles';
    RAISE NOTICE '  3. Identify ground truth papers (after references populated)';
    RAISE NOTICE '  4. Test recommendation generation';
    RAISE NOTICE '';
    RAISE NOTICE '═══════════════════════════════════════════════════════════════';
END $$;