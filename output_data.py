import asyncio
import db

async def generate_all_outputs():
  a = await db.select_budget_by_genre_by_year()
  b = await db.select_revenue_by_genre_by_year()
  c = await db.select_profit_by_genre_by_year()
  d = await db.select_popularity_by_genre_by_year()
  e = await db.select_budget_by_company_by_year()
  f = await db.select_revenue_by_company_by_year()
  g = await db.select_profit_by_company_by_year()
  h = await db.select_popularity_by_company_by_year()
  i = await db.select_releases_by_company_by_genre_by_year()
#asyncio.run(generate_all_outputs())

async def get_budget_by_genre_by_year():
  results = await db.select_budget_by_genre_by_year()
  return results
#asyncio.run(get_budget_by_genre_by_year())

async def get_revenue_by_genre_by_year():
  results = await db.select_revenue_by_genre_by_year()
  return results
#asyncio.run(get_revenue_by_genre_by_year())

async def get_profit_by_genre_by_year():
  results = await db.select_profit_by_genre_by_year()
  return results
#asyncio.run(get_profit_by_genre_by_year())

async def get_popularity_by_genre_by_year():
  results = await db.select_popularity_by_genre_by_year()
  return results
#asyncio.run(get_popularity_by_genre_by_year())

async def get_budget_by_company_by_year():
  results = await db.select_budget_by_company_by_year()
  return results
#asyncio.run(get_budget_by_company_by_year())

async def get_revenue_by_company_by_year():
  results = await db.select_revenue_by_company_by_year()
  return results
#asyncio.run(get_revenue_by_company_by_year())

async def get_profit_by_company_by_year():
  results = await db.select_profit_by_company_by_year()
  return results
#asyncio.run(get_profit_by_company_by_year())

async def get_popularity_by_company_by_year():
  results = await db.select_popularity_by_company_by_year()
  return results
#asyncio.run(get_popularity_by_company_by_year())

async def get_releases_by_company_by_genre_by_year():
  results = await db.select_releases_by_company_by_genre_by_year()
  return results
#asyncio.run(get_releases_by_company_by_genre_by_year())