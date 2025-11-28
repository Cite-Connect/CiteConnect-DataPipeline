import asyncpg
import asyncio

async def run_migration():
    conn = await asyncpg.connect(
        host='aws-1-us-east-2.pooler.supabase.com',
        port=5432,
        database='postgres',
        user='postgres.wvvogncqrqzfbfztkwfo',
        password='CiteConnect-Dev'
    )
    
    with open('/Users/dennis_m_jose/Documents/GitHub/CiteConnect-DataPipeline/scripts/schema_migration_v2.sql', 'r') as f:
        sql = f.read()
    
    await conn.execute(sql)
    await conn.close()
    print("✅ Migration complete")

asyncio.run(run_migration())