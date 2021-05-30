import asyncio
import db

# Function for generating outputs to all 9 questions
async def generate_all_outputs():
  await db.select_budget_by_genre_by_year()
  await db.select_revenue_by_genre_by_year()
  await db.select_profit_by_genre_by_year()
  await db.select_popularity_by_genre_by_year()
  await db.select_budget_by_company_by_year()
  await db.select_revenue_by_company_by_year()
  await db.select_profit_by_company_by_year()
  await db.select_popularity_by_company_by_year()
  await db.select_releases_by_company_by_genre_by_year()
asyncio.run(generate_all_outputs())

# Functions to generate output for individual questions
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