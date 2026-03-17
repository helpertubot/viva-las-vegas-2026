# Feature Spec: Live Scores, ESPN Bracket Scoring, Tiebreaker

## Overview
Add live NCAA tournament results, ESPN-style bracket scoring, a leaderboard, and a championship tiebreaker.

## ESPN API
- Base: `https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard`
- Query: `?dates=YYYYMMDD&groups=100&limit=50`
- Tournament games have notes like: `"NCAA Men's Basketball Championship - East Region - 1st Round"`
- Each game has: competitors[].team.shortDisplayName, competitors[].curatedRank.current (seed), competitors[].score, competitions[0].status.type.description (Scheduled/In Progress/Final), competitions[0].status.type.completed (bool)
- Games exist on: March 19-20 (R1), March 21-22 (R2), March 27-28 (Sweet 16), March 29-30 (Elite 8), April 4 (Final Four), April 6 (Championship)

## Bracket Scoring (ESPN Standard)
- Round 1 (64→32): 10 points per correct pick
- Round 2 (32→16): 20 points per correct pick
- Sweet 16 (16→8): 40 points per correct pick
- Elite 8 (8→4): 80 points per correct pick
- Final Four (4→2): 160 points per correct pick
- Championship (2→1): 320 points per correct pick
- Max possible: 1920 points

## Tiebreaker
- Each bracket includes a tiebreaker: predicted total combined score of the championship game
- Closest to actual combined score wins tiebreaker

## Backend Changes (api_server.py - PostgreSQL with psycopg2)
1. New table `tournament_results`:
   - game_key TEXT PRIMARY KEY (e.g. "R1_E_1v16" or use matchup string)
   - round INTEGER (1-6)
   - region TEXT
   - seed_high INTEGER
   - seed_low INTEGER  
   - winner_name TEXT
   - winner_seed INTEGER
   - home_score INTEGER
   - away_score INTEGER
   - game_state TEXT (pre/in/final)
   - updated_at DOUBLE PRECISION
   
2. New endpoint `GET /api/tournament/results` - returns all game results
3. New endpoint `POST /api/admin/tournament/refresh` - manually trigger refresh (admin only)
4. New endpoint `GET /api/leaderboard` - returns scored brackets with rankings
5. Background task or on-demand fetch from ESPN API to update tournament_results
6. Add `tiebreaker_score` INTEGER column to brackets table
7. Scoring function that compares each bracket's picks against tournament_results

## Frontend Changes (app.js)
1. Add tiebreaker input field when creating/editing bracket (number input for predicted total score)
2. Add Leaderboard section showing all brackets ranked by score
3. Show bracket scores on Home page member cards
4. When viewing a bracket, show which picks are correct (green), wrong (red), or pending (gray)
5. Show live game scores somewhere accessible

## Key Constraints
- Database is PostgreSQL on Neon (use %s placeholders, not ?)
- Connection string: postgresql://neondb_owner:npg_bkNlfWGCVD95@ep-dawn-lake-am5wefih-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require
- The bracket `picks` field is a JSON string with keys like "R1_E_1v16" mapping to the picked team name
- API base URL logic in app.js: empty string '' when not localhost
- All existing functionality must continue working
- Static files served via FastAPI FileResponse
