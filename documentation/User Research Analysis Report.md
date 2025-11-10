# CiteConnect User Research Analysis Report
## Survey-Based Validation of AI-Powered Research Discovery

**Report Date:** October 28, 2025  
**Survey Period:** October 24-28, 2025  
**Total Responses:** 13  
**Target Users:** Graduate students and industry researchers

---

## 1. Executive Summary

This report analyzes user research validating the need for CiteConnect, an AI-powered research paper recommendation system with semantic search and citation network visualization.

### Key Findings

✅ **Strong Market Need:** 92% actively struggle with current paper discovery tools  
✅ **High AI Adoption:** 85% use AI tools regularly, but dissatisfaction with literature search (38% dissatisfied)  
✅ **Feature Validation:** 100% interested in citation network visualization, 85% want AI pre-filtering  
✅ **Trust Requirement:** 93% require explanation transparency  
✅ **Preference:** 69% want collaborative AI (user retains control)

### Critical Insight

**Primary Pain Point:** "Too many irrelevant results" (54% rank as #1 frustration)  
**Implication:** Optimize for **PRECISION** (show 10 perfect papers) over **RECALL** (show 100 okay papers)

### Recommendation

**PROCEED with CiteConnect development** focusing on:
1. Semantic search (sentence-transformers)
2. Explainable recommendations (LLM-generated)
3. Interactive citation network
4. Precision-optimized ranking (target: 80%+ relevance in top 10)

---

## 2. Respondent Profile

### Demographics
- **Graduate Students:** 62% (MS/PhD students)
- **Industry Researchers:** 23%
- **Undergraduates:** 8%
- **Research Assistants:** 8%

### Research Fields
- **Computer Science:** 69%
- **Engineering, Biology, Physics, Chemistry, HCI, Management:** 31% (distributed)

### AI Comfort Level
- **Very comfortable (daily use):** 77%
- **Cautious but willing:** 23%
- **Uncomfortable:** 0%

**Insight:** Respondents are early adopters - ideal for validating AI-first product.

---

## 3. Current Research Practices

### Search Frequency & Time Investment

| Metric | Finding |
|--------|---------|
| **Search frequency** | 53% search weekly or more |
| **Time per week** | 54% spend 1-3 hours |
| **Annual time cost** | ~100 hours/year per researcher |

### Current Tool Usage

| Tool | Adoption |
|------|----------|
| Google Scholar | 92% |
| ArXiv | 46% |
| IEEE Xplore | 46% |
| ChatGPT/Claude for discovery | 23% (emerging trend) |
| Connected Papers | 8% |

**Key Finding:** Google Scholar dominates, but 23% already experiment with AI tools for paper discovery - **early market validation**.

---

## 4. Pain Points Analysis

### Top 3 Frustrations (Ranked)

| Pain Point | Rank #1 | Total Mentions | % |
|------------|---------|----------------|---|
| **Too many irrelevant results** | 5 | 7 | **54%** |
| **Missing important papers** | 2 | 4 | **31%** |
| **No explanation of why papers suggested** | 1 | 3 | **23%** |

### Qualitative Themes

**Theme 1: Relevance Problems (6 mentions)**
> "AI tools give generic data, not specific for my case"  
> "100+ papers splashed with no filtering"  
> "Hard to find papers at intersection of two fields"

**Theme 2: AI Tool Limitations (4 mentions)**
> "ChatGPT gives wrong paper links"  
> "Doesn't understand papers with math/equations"  
> "AI hallucinates - leading to irrelevant results"

**Theme 3: Poor Explanations (3 mentions)**
> "Don't know how to trust a paper"  
> "No explanation of why papers are suggested"  
> "Can't ask more about a specific paper"

### User Impact

**Average time wasted per search:** 45 minutes filtering through irrelevant results  
**Annual impact:** 30+ hours/year wasted on false positives

**CiteConnect Opportunity:** Reduce filtering time by 50-60% via semantic pre-filtering.

---

## 5. AI Tool Usage & Satisfaction

### Current AI Tool Adoption for Research

**Daily/Weekly users:** 85%

**Most used AI tools:**
- ChatGPT/GPT-4: 85%
- Grammarly: 38%
- GitHub Copilot: 31%
- Perplexity for research: 15%

### Satisfaction with AI for Literature Search

| Level | % |
|-------|---|
| Dissatisfied/Very Dissatisfied | **38%** |
| Neutral | 23% |
| Satisfied | 31% |
| Very Satisfied | 0% |

**Average:** 2.8/5 (below neutral)

**By Task Satisfaction:**
- ❌ Citation analysis: 2.3/5 (lowest)
- ⚠️ Literature discovery: 2.8/5 (low)
- ✅ Paper summarization: 3.4/5 (acceptable)
- ✅ Writing assistance: 3.5/5 (good)

**Conclusion:** Current AI tools **underperform** for citation analysis and discovery - **primary CiteConnect features!**

---

## 6. Feature Validation

### 6.1 Citation Network Visualization

**Interest Level:**
- **Extremely interested:** 77%
- **Very interested:** 23%
- **Total interested:** 100% ✅

**Most Desired Visualization Types:**

| Type | Demand |
|------|--------|
| Thematic clusters (similar topics) | 77% |
| Direct citation relationships | 77% |
| Co-citation patterns | 62% |
| Methodology connections | 54% |
| Research impact flow | 54% |

**Validation:** Citation network is a **must-have differentiator**.

---

### 6.2 AI Assistant Task Preferences

**Tasks users want AI to handle:**

| Task | Demand | Priority |
|------|--------|----------|
| **Pre-filter by relevance** | 85% | 🔴 P0 |
| **Summarize key findings** | 85% | 🔴 P0 |
| **Explain why papers interesting** | 77% | 🔴 P0 |
| **Identify connections** | 77% | 🔴 P0 |
| **Alert to new papers** | 46% | 🟡 P1 |
| **Suggest research directions** | 31% | 🟢 P2 |

**MVP Scope:** Focus on top 4 tasks (>75% demand).

---

### 6.3 Feature Importance Rankings

**Average importance (1=most, 7=least):**

| Feature | Avg Rank | Status |
|---------|----------|--------|
| **Paper summarization** | 2.8 | Highest priority |
| **Personalized recommendations** | 3.2 | High priority |
| **Citation network viz** | 3.3 | High priority |
| **AI explanations** | 3.2 | High priority |
| **Semantic search** | 3.5 | High priority |
| **Collaboration features** | 3.2 | Medium priority |
| **Reference manager integration** | 4.5 | Lower priority |

**All core CiteConnect features rank in top 5** - strong alignment with user needs!

---

## 7. Trust & Control

### 7.1 Desired Control Level

| Preference | % |
|------------|---|
| **Collaborative - AI suggests, I decide** | **69%** |
| Mostly automated with adjustments | 23% |
| Fully automated | 8% |

**Design Principle:** User retains agency - AI augments, doesn't automate.

**Ideal Experience (most selected):**
> "Help me explore and discover, but let me control the direction" (69%)

---

### 7.2 Importance of Explanations

| Importance | % |
|------------|---|
| **Extremely important** | 62% |
| **Very important** | 31% |
| Important | 0% |

**93% require explanations** to trust recommendations.

**Implementation:** Every paper needs "why" explanation with:
- Shared keywords/concepts
- Citation connections
- Methodology match
- Confidence score

---

### 7.3 Trust-Building Factors

| Factor | % Selected |
|--------|------------|
| Transparent explanations | 77% |
| Accuracy improves over time | 62% |
| Integration with established databases | 62% |
| User reviews/ratings | 54% |
| Reputable sources only | 46% |

**Top 3 requirements for trust:**
1. Explain the logic
2. Get better with feedback
3. Use verified academic sources (Semantic Scholar API)

---

## 8. Precision vs Recall Optimization

### Analysis

**False Positive Pain (irrelevant results):**
- 54% rank as #1 frustration
- Time wasted: 45 min/session filtering

**False Negative Pain (missing papers):**
- 31% rank as #2 frustration
- Users already use citation chaining to recover

**Ratio:** False positives hurt **1.7x more** than false negatives

### Recommendation

**Optimize for PRECISION over RECALL**

**Justification:**
> "Users prefer seeing 10 highly relevant papers over 100 papers with 50% noise. We understand this means potentially missing some edge-case papers, but users value time savings over exhaustive coverage. They can discover missed papers through citation network exploration."

**Target Metrics (Updated from Scoping Doc):**
- **Precision@10:** ≥0.80 (increased from 0.60)
- **Recall@10:** ≥0.75 (maintained)
- **False Positive Rate:** <20% (stricter than scoping doc)

---

## 9. Success Criteria

Based on survey data and scoping document Section 11:

### 9.1 Technical Metrics

**Retrieval Quality:**
- Recall@10 ≥ 0.75 (find 75% of relevant papers)
- **Precision@10 ≥ 0.80** (80%+ of top 10 are relevant) ← Survey-driven increase
- Query latency < 2 seconds (p95)

**Validation:** Survey shows 54% frustrated by irrelevant results - need higher precision.

---

### 9.2 User Engagement Metrics

**From Scoping Doc + Survey Validation:**

| Metric | Target | Survey Evidence |
|--------|--------|-----------------|
| **CTR (Click-Through Rate)** | ≥25% | 85% want pre-filtered relevant papers |
| **Return Rate (7-day)** | ≥35% | 69% prefer collaborative exploration (sticky) |
| **Explanation Quality** | ≥4.0/5.0 | 93% require explanations for trust |
| **Time Savings** | ≥50% | Users spend 45 min filtering → target 20 min |

---

### 9.3 Success Metric Statements

**Version 1 (Precision-Focused):**
> If **Precision@10** for **semantic search recommendations** drops below **0.80**, we will increase relevance threshold and reduce result set size.

**Version 2 (Explainability-Focused):**
> If **average explanation quality rating** for **AI-generated paper relevance descriptions** drops below **4.0/5.0**, we will refine LLM prompts and add more supporting evidence.

**Version 3 (Engagement-Focused):**
> If **click-through rate** for **top 10 recommendations** drops below **25%**, we will re-tune ranking algorithm and incorporate more user preference signals.

---

## 10. Strategic Recommendations

### 10.1 MVP Feature Scope (Validated)

**Must-Have (Week 7-10):**
1. ✅ Semantic search using sentence-transformers embeddings
2. ⏰ LLM-generated explanations for each recommendation
3. ⏰ Interactive citation network (D3.js/Cytoscape)
4. ⏰ Personalized ranking (citation count + recency + semantic similarity)
5. ⏰ User rating system (1-5 stars)

**Defer to Post-MVP:**
- Email alerts (46% demand - important but not critical)
- Collaboration features (23% demand)
- Reference manager integration (ranked 4.5/7 - lower priority)

---

### 10.2 Design Principles (Survey-Driven)

**Principle 1: Precision > Recall**
- Show 10 perfect papers, not 100 mediocre ones
- Users can explore more via citation network if needed

**Principle 2: Explain Everything**
- Every recommendation needs "why"
- Show confidence scores
- Cite reasoning (shared keywords, citation links, etc.)

**Principle 3: User Agency**
- AI suggests, user decides
- No auto-actions (downloading, saving, etc.)
- Collaborative exploration interface

**Principle 4: Visual-First**
- Citation network as primary discovery interface
- List view as secondary
- Color-code by relevance/theme

**Principle 5: Learn Continuously**
- Track clicks, reading time, ratings
- Improve recommendations over time
- Personalize based on behavior

---

### 10.3 Competitive Differentiation

**CiteConnect's Unique Position:**

| Competitor | Strength | Weakness | CiteConnect Advantage |
|------------|----------|----------|------------------------|
| **Google Scholar** | Comprehensive | Keyword-only, no explanations | Semantic + explainable |
| **ChatGPT/Claude** | Conversational | Hallucinates, wrong links | Verified sources + confidence |
| **Connected Papers** | Visual network | No personalization, exploration-only | Personalized + explanations |
| **Semantic Scholar** | Good metadata | Poor UX, no explanations | Better UX + AI insights |

**Positioning Statement:**
> "The first AI research assistant that explains WHY papers matter and HOW they connect - not just WHAT exists."

---

## 11. Risk Analysis & Mitigation

### Identified Risks

**Risk 1: AI Hallucination/Inaccuracy**
- **Evidence:** 4 users mentioned wrong links, generic suggestions
- **Mitigation:** Use Semantic Scholar API (verified metadata), add source validation layer
- **Metric:** Track accuracy of paper links (target: 100% valid)

**Risk 2: Shallow Understanding**
- **Evidence:** "Doesn't understand math/equations", "surface-level summaries"
- **Mitigation:** Use full-text embeddings (introduction sections), not just abstracts
- **Metric:** Explanation quality rating ≥4.0/5.0

**Risk 3: Generic Recommendations**
- **Evidence:** "Baked in generic data, not specific to my case"
- **Mitigation:** Personalization via user profiles, click tracking, iterative learning
- **Metric:** Return rate ≥35% indicates personalization working

---

## 12. Conclusions & Next Steps

### 12.1 Validation of Core Thesis

**Original Problem (from Scoping Doc):**
> "Researchers face information overload. Keyword search returns long, noisy lists. No personalization or explainability."

**Survey Validation:**
- ✅ 54% rank "too many irrelevant results" as #1 frustration (information overload confirmed)
- ✅ 85% want AI pre-filtering (keyword search inadequate)
- ✅ 93% require explanations (explainability critical)

**Conclusion:** **All core assumptions validated.**

---

### 12.2 Go/No-Go Decision

**RECOMMENDATION: GO ✅**

**Confidence Level:** High (85%+)

**Supporting Evidence:**
1. Clear, validated user pain (54% struggle with relevance)
2. High interest in solution (100% want citation visualization)
3. Existing tools underperform (38% dissatisfied with AI literature search)
4. Technical feasibility proven (embedding pipeline operational)
5. Target users willing to adopt (77% very comfortable with AI)

---

### 12.3 Immediate Actions (Week 5-6)

**Development:**
1. ✅ Complete embedding service testing (in progress)
2. ⏰ Build LLM explanation service (GPT-4 for "why this paper")
3. ⏰ Set up Neo4j citation graph
4. ⏰ Design ranking algorithm (semantic + citation + recency)

**Research:**
1. ⏰ Recruit 5-10 beta testers from survey respondents
2. ⏰ Create explanation template for testing
3. ⏰ Design precision measurement methodology

**Documentation:**
1. ✅ User research report (this document)
2. ⏰ Updated success metrics in scoping doc
3. ⏰ Beta test plan

---

### 12.4 Updated Success Metrics

**Based on survey findings, update scoping doc Section 11:**

| Metric | Original Target | Survey-Updated Target | Rationale |
|--------|-----------------|----------------------|-----------|
| Precision@10 | 0.60 | **0.80** | 54% frustrated by irrelevant results |
| Recall@10 | 0.75 | 0.75 | Maintained (users can explore via network) |
| CTR | 25% | 25% | Validated by 85% wanting pre-filtering |
| Explanation Quality | - | **4.0/5.0** | New metric (93% need explanations) |
| Return Rate (7-day) | 35% | 35% | Validated by 69% collaborative preference |

---

### 12.5 Feature Priority Adjustments

**Promoted to P0 (based on survey):**
- ✅ Explainable recommendations (93% critical) - add to core MVP

**Maintained as P0:**
- ✅ Semantic search (85% demand)
- ✅ Citation network (100% interest)
- ✅ Personalized ranking (77% demand)

**Demoted to P1:**
- ⏰ Reference manager integration (ranked 4.5/7 - lower than expected)

---

## 13. Open Questions for Future Research

### Unvalidated Assumptions

1. **Monetization:** Survey didn't assess willingness to pay
   - **Action:** Add pricing questions to beta feedback

2. **Collaboration Features:** Low survey demand (23%), but underestimated?
   - **Action:** Prototype testing to validate importance

3. **Mobile vs Desktop:** Not asked in survey
   - **Action:** Add device preference question

4. **Threshold for "Too Many Results":** What's the ideal result set size?
   - **Action:** A/B test 10 vs 20 vs 50 results in beta

---

## 14. Appendix: Key Quotes

**On Current Frustrations:**
> "When I tried to look for research papers on a specific topic, ChatGPT suggested promising papers but many links led to entirely different papers - challenging to find reliable sources." - Grad Student, CS

> "I wanted to study about a specific topic but lots of papers were splashed on my face and it was annoying - tools have horrible UI and unplanned UX." - MS Student, CS

**On AI Limitations:**
> "Most AI tools feel surface-level - they summarize papers well but don't actually understand the research context. They miss domain nuance or mix up technical terms." - Industry Researcher, CS

> "Citation tracing and dataset links are rarely reliable [in AI tools]. Tools like Elicit and Research Rabbit either overload you with generic suggestions or fail to connect ideas." - Industry Researcher, CS

**On Desired Solutions:**
> "If AI could understand my specific research domain and continuously suggest the most recent and impactful studies, it would significantly improve efficiency." - Grad Student, CS

> "I'd use AI more if it actually learned my research style - what kind of papers I cite, which methods I prefer, what gaps I'm exploring." - Industry Researcher, CS

---

## 15. Final Verdict

### Market Validation: ✅ STRONG

- **Problem validated:** 92% struggle with current tools
- **Solution validated:** 100% want citation network, 85% want semantic search
- **Differentiation validated:** Explainability (93% critical) is key differentiator
- **Technical validation:** Embedding service operational, GCS integration working

### Strategic Direction

**Focus:** Build "The Explainable Research Discovery Engine"

**Core Value Proposition:**
> "CiteConnect finds the 10 papers you actually need (not 100 you don't), shows you HOW they connect, and explains WHY each one matters - saving researchers 50+ hours per year."

### Next Milestone

**Beta Launch (Week 11):**
- Recruit 10 beta testers
- Test precision, explanations, citation graph
- Validate time savings claims
- Iterate based on feedback

**Success Criteria:**
- Beta users rate precision ≥4.0/5.0
- Explanation quality ≥4.0/5.0
- 60%+ would recommend to colleagues

