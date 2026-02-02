import streamlit as st
import sqlite3
import pandas as pd
import uuid
import datetime
import time
from database.db import get_connection
from modeling.elo import EloModel
from config import STARTING_ELO_2025_26
TEAMS = STARTING_ELO_2025_26

# Page Config
st.set_page_config(page_title="TrueLine", layout="wide")
st.title("🏀 TrueLine")
st.caption("v2.1.0 - Cloud DB Edition")

# --- DATABASE UTILS ---

def get_teams_map():
    """Fetch teams from DB. If empty, populate from config."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM teams")
    rows = cursor.fetchall()
    
    if not rows:
        # Populate DB with teams
        print("Populating teams table...")
        for name in TEAMS.keys():
            cursor.execute("INSERT OR IGNORE INTO teams (name) VALUES (?)", (name,))
        conn.commit()
        # Fetch again
        cursor.execute("SELECT id, name FROM teams")
        rows = cursor.fetchall()
    
    conn.close()
    return {row[1]: row[0] for row in rows}

def insert_game(date, home_id, away_id, home_odds=None, away_odds=None):
    """Insert game and odds into DB."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Generate stats
        ext_id = str(uuid.uuid4())
        
        # Insert Game
        cursor.execute("""
            INSERT INTO games (external_id, start_time, home_team_id, away_team_id, status)
            VALUES (?, ?, ?, ?, 'SCHEDULED')
        """, (ext_id, date, home_id, away_id))
        
        game_id = cursor.lastrowid
        
        # Insert Odds if provided
        if home_odds and away_odds:
            cursor.execute("""
                INSERT INTO odds_snapshots (game_id, book, pulled_at, home_dec, away_dec)
                VALUES (?, 'Manual', CURRENT_TIMESTAMP, ?, ?)
            """, (game_id, home_odds, away_odds))
            
        conn.commit()
        return True, f"Game ID {game_id} created successfully."
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()

# Initialize Teams Map
TEAMS_MAP = get_teams_map()
TEAM_NAMES = sorted(list(TEAMS_MAP.keys()))

# --- BETTING UTILS ---
def place_bet(game_id, side, odds, stake, edge=0, ev=0):
    conn = get_connection()
    c = conn.cursor()
    try:
        # Check result immediately if game is final?
        # For now, just insert.
        c.execute("""
            INSERT INTO paper_bets (game_id, side, odds, book, stake, potential_payout, edge, ev, result)
            VALUES (?, ?, ?, 'Manual', ?, ?, ?, ?, 'PENDING')
        """, (game_id, side, odds, stake, stake*odds, edge, ev))
        conn.commit()
        return True, "Bet placed successfully!"
    except Exception as e:
        return False, str(e)
    finally:
        conn.close()

def resolve_bets():
    """Auto-resolve pending bets based on game status."""
    conn = get_connection()
    c = conn.cursor()
    
    # Find pending bets where game is FINAL
    query = """
        SELECT b.id, b.side, g.home_score, g.away_score
        FROM paper_bets b
        JOIN games g ON b.game_id = g.id
        WHERE b.result = 'PENDING' AND g.status = 'FINAL'
    """
    c.execute(query)
    rows = c.fetchall()
    
    count = 0
    for row in rows:
        bid, side, h_s, a_s = row
        won = False
        if side == 'HOME' and h_s > a_s: won = True
        elif side == 'AWAY' and a_s > h_s: won = True
        
        # Update
        res_str = 'WIN' if won else 'LOSS'
        # PnL calculation done in SQL update usually, or logic here
        # We need to fetch odds/stake to calc PnL.
        # Let's just update result 'WIN'/'LOSS' and handle PnL in view or update query
        c.execute("UPDATE paper_bets SET result = ? WHERE id = ?", (res_str, bid))
        count += 1
        
    # Update PnL for WINS/LOSSES
    c.execute("UPDATE paper_bets SET pnl = (potential_payout - stake) WHERE result = 'WIN' AND pnl IS NULL")
    c.execute("UPDATE paper_bets SET pnl = -stake WHERE result = 'LOSS' AND pnl IS NULL")
    
    conn.commit()
    conn.close()
    return count

# --- UI TABS ---
tab1, tab2, tab3, tab4 = st.tabs(["🔮 Predict", "📝 Bets", "➕ Add Game", "📊 Data"])

# --- HELPERS ---
def american_to_decimal(us_odds):
    if not us_odds: return 0.0
    if us_odds > 0:
        return (us_odds / 100) + 1
    else:
        return (100 / abs(us_odds)) + 1

def decimal_to_american(dec_odds):
    if not dec_odds or dec_odds <= 1.0: return 0
    if dec_odds >= 2.0:
        return int((dec_odds - 1) * 100)
    else:
        return int(-100 / (dec_odds - 1))

def format_odds(dec_val, fmt):
    if fmt == "American":
        us = decimal_to_american(dec_val)
        return f"+{us}" if us > 0 else str(us)
    return f"{dec_val:.2f}"

# --- TAB 1: PREDICT ---
with tab1:
    st.header("Match Prediction")
    
    # Sidebar Config
    odds_format = st.sidebar.radio("Odds Format", ["American", "Decimal"])
    
    mode = st.radio("Analysis Mode", ["Single Match", "Daily Schedule"], horizontal=True)
    
    if mode == "Single Match":
        col1, col2 = st.columns(2)
        with col1:
            home_team = st.selectbox("Home Team", TEAM_NAMES)
        with col2:
            away_team = st.selectbox("Away Team", TEAM_NAMES, index=1 if len(TEAM_NAMES)>1 else 0)
            
        date_input = st.date_input("Game Date", datetime.date.today())
        
        # Odds Input
        st.subheader(f"Market Odds ({odds_format})")
        c1, c2 = st.columns(2)
        
        if odds_format == "American":
            with c1:
                in_home = st.number_input("Home Odds (US)", value=-110, step=5)
            with c2:
                in_away = st.number_input("Away Odds (US)", value=-110, step=5)
            # Convert for calculation
            p_home_odds = american_to_decimal(in_home)
            p_away_odds = american_to_decimal(in_away)
        else:
            with c1:
                p_home_odds = st.number_input("Home Odds (Decimal)", min_value=1.01, max_value=100.0, value=1.90, step=0.01)
            with c2:
                p_away_odds = st.number_input("Away Odds (Decimal)", min_value=1.01, max_value=100.0, value=1.90, step=0.01)
        
        # Stake Input
        st.subheader("Bet Sizing")
        stake_amount = st.number_input("Stake Amount ($)", min_value=1.0, value=1.0, step=1.0)

        if st.button("Analyze Match", type="primary"):
            if home_team == away_team:
                st.error("Home and Away teams must be different.")
            else:
                home_id = TEAMS_MAP.get(home_team)
                away_id = TEAMS_MAP.get(away_team)
                
                if not home_id or not away_id:
                    st.error("Error: Could not find Team IDs. Please check database.")
                else:
                    model = EloModel()
                    model.load_ratings()
                    home_elo = model.get_team_rating(home_id)
                    away_elo = model.get_team_rating(away_id)
                    win_prob, _ = model.predict_game(home_elo, away_elo)
                
                # Display Result
                st.markdown("---")
                res_col1, res_col2 = st.columns(2)
                ev_home = 0
                ev_away = 0
                
                with res_col1:
                    st.metric("Home Win Probability", f"{win_prob:.1%}", help=f"{home_team}")
                    if p_home_odds:
                        ev_home = (win_prob * p_home_odds) - 1
                        st.metric("Home EV", f"{ev_home:.2%}", delta_color="normal" if ev_home > 0 else "off")
                
                with res_col2:
                    st.metric("Away Win Probability", f"{(1-win_prob):.1%}", help=f"{away_team}")
                    if p_away_odds:
                        ev_away = ((1 - win_prob) * p_away_odds) - 1
                        st.metric("Away EV", f"{ev_away:.2%}", delta_color="normal" if ev_away > 0 else "off")
                
                st.markdown("### Recommendation")
                rec_side = None
                rec_odds = 0
                rec_ev = 0
                
                disp_h = format_odds(p_home_odds, odds_format)
                disp_a = format_odds(p_away_odds, odds_format)
                
                if p_home_odds and p_away_odds:
                    if ev_home > 0:
                        st.success(f"🔥 **BET HOME**: {home_team} @ {disp_h} (EV: {ev_home:.2%})")
                        rec_side = "HOME"
                        rec_odds = p_home_odds
                        rec_ev = ev_home
                    elif ev_away > 0:
                        st.success(f"🔥 **BET AWAY**: {away_team} @ {disp_a} (EV: {ev_away:.2%})")
                        rec_side = "AWAY"
                        rec_odds = p_away_odds
                        rec_ev = ev_away
                    else:
                        st.info("No value found on either side. Pass.")
                
                if rec_side:
                    if st.button(f"Place Bet on {rec_side} (${stake_amount})"):
                        # Ensure Game Exists Logic
                        conn = get_connection()
                        cur = conn.cursor()
                        cur.execute("SELECT id FROM games WHERE date(start_time) = ? AND home_team_id = ? AND away_team_id = ?", (date_input, home_id, away_id))
                        row = cur.fetchone()
                        gid = row[0] if row else None
                        if not gid:
                            s, msg = insert_game(date_input, home_id, away_id)
                            if s: gid = cur.lastrowid # Note: lastrowid reliability dependent on flow
                            # Re-fetch strictly to be safe
                            cur.execute("SELECT id FROM games WHERE date(start_time) = ? AND home_team_id = ? AND away_team_id = ?", (date_input, home_id, away_id))
                            row_new = cur.fetchone()
                            if row_new: gid = row_new[0]
                        conn.close()

                        if gid:
                            success, msg = place_bet(gid, rec_side, rec_odds, stake_amount, 0, rec_ev)
                            if success: st.success("Bet Placed!")
                            else: st.error(f"Failed: {msg}")
                        else:
                            st.error("Could not find/create game record.")

    elif mode == "Daily Schedule":
        sch_date = st.date_input("Select Date", datetime.date.today())
        
        # Fetch Games
        conn = get_connection()
        games = pd.read_sql("""
            SELECT g.id, g.start_time, h.name as home, a.name as away 
            FROM games g
            JOIN teams h ON g.home_team_id = h.id
            JOIN teams a ON g.away_team_id = a.id
            WHERE date(g.start_time) = ?
            ORDER BY g.start_time
        """, conn, params=(sch_date,))
        conn.close()
        
        if games.empty:
            st.warning("No games found in database for this date.")
        else:
            st.markdown(f"**Found {len(games)} games.** Enter odds below ({odds_format}):")
            
            # Form for Batch Analysis
            with st.form("batch_odds_form"):
                inputs = []
                for idx, row in games.iterrows():
                    st.markdown(f"**{row['home']} vs {row['away']}**")
                    c1, c2 = st.columns(2)
                    
                    if odds_format == "American":
                        with c1:
                            val_h = st.number_input(f"Home ({row['home']})", value=-110, step=5, key=f"h_{row['id']}")
                        with c2:
                            val_a = st.number_input(f"Away ({row['away']})", value=-110, step=5, key=f"a_{row['id']}")
                        h_dec = american_to_decimal(val_h)
                        a_dec = american_to_decimal(val_a)
                    else:
                        with c1:
                            val_h = st.number_input(f"Home ({row['home']})", 1.01, 100.0, 1.90, key=f"h_{row['id']}")
                        with c2:
                            val_a = st.number_input(f"Away ({row['away']})", 1.01, 100.0, 1.90, key=f"a_{row['id']}")
                        h_dec = val_h
                        a_dec = val_a
                        
                    st.divider()
                    inputs.append({
                        'id': row['id'], 
                        'home': row['home'], 
                        'away': row['away'], 
                        'h_odds': h_dec, 
                        'a_odds': a_dec,
                        'disp_h': format_odds(h_dec, odds_format),
                        'disp_a': format_odds(a_dec, odds_format)
                    })
                
                submitted = st.form_submit_button("Analyze Daily Slate")
                
            if submitted:
                model = EloModel()
                model.load_ratings()
                results = []
                
                for item in inputs:
                    mid = item['id']
                    hid = TEAMS_MAP.get(item['home'])
                    aid = TEAMS_MAP.get(item['away'])
                    
                    if hid and aid:
                        h_elo = model.get_team_rating(hid)
                        a_elo = model.get_team_rating(aid)
                        win_prob, _ = model.predict_game(h_elo, a_elo)
                        
                        # Calculate Implied Probabilities
                        h_dec = item['h_odds']
                        a_dec = item['a_odds']
                        
                        impl_h = 1 / h_dec if h_dec > 0 else 0
                        impl_a = 1 / a_dec if a_dec > 0 else 0
                        
                        edge_h = win_prob - impl_h
                        edge_a = (1-win_prob) - impl_a
                        
                        h_ev = (win_prob * h_dec) - 1
                        a_ev = ((1-win_prob) * a_dec) - 1
                        
                        rec = "PASS"
                        bet_side = None
                        bet_odds = 0
                        bet_edge = 0
                        bet_ev = 0
                        
                        if h_ev > 0: 
                            rec = f"BET HOME ({item['home']}) @ {item['disp_h']}"
                            bet_side = "HOME"
                            bet_odds = h_dec
                            bet_edge = edge_h # storing edge diff
                            bet_ev = h_ev
                        elif a_ev > 0: 
                            rec = f"BET AWAY ({item['away']}) @ {item['disp_a']}"
                            bet_side = "AWAY"
                            bet_odds = a_dec
                            bet_edge = edge_a
                            bet_ev = a_ev
                        
                        results.append({
                            "Game": f"{item['home']} vs {item['away']}",
                            "Home Odds": item['disp_h'],
                            "Away Odds": item['disp_a'],
                            "Home Prob": f"{win_prob:.1%}",
                            "Away Prob": f"{(1-win_prob):.1%}",
                            "Home Edge": f"{edge_h:.1%}",
                            "Away Edge": f"{edge_a:.1%}",
                            "Home EV": f"{h_ev:.1%}",
                            "Away EV": f"{a_ev:.1%}",
                            "Recommendation": rec,
                            "raw": {
                                "game_id": mid,
                                "side": bet_side,
                                "odds": bet_odds,
                                "edge": bet_edge,
                                "ev": bet_ev,
                                "stake": 1.0 # Default stake
                            }
                        })
                
                st.session_state['daily_results'] = results

            if 'daily_results' in st.session_state:
                st.markdown("### Analysis Results")
                
                # Global Stake for Batch?
                # For now just use the default from input or 1.0
                
                for i, res in enumerate(st.session_state['daily_results']):
                    with st.container():
                        c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
                        with c1:
                            st.write(f"**{res['Game']}**")
                            st.caption(f"H: {res['Home Odds']} ({res['Home EV']}) | A: {res['Away Odds']} ({res['Away EV']})")
                        
                        with c2:
                            st.write(f"Edge: {res['Home Edge'] if 'HOME' in res.get('Recommendation','') else res['Away Edge']}")
                            
                        with c3:
                            if "BET" in res['Recommendation']:
                                st.success(res['Recommendation'].replace("BET ", ""))
                            else:
                                st.info("PASS")

                        with c4:
                            raw = res['raw']
                            if raw['side']:
                                b_key = f"btn_bet_batch_{i}_{raw['game_id']}"
                                if st.button(f"Bet {raw['side']}", key=b_key):
                                    # Use a default stake of 1 unit or fetch from somewhere?
                                    # The Sidebar has stake_amount, but it's inside Single Match if block?
                                    # No, stake_amount is defined line 191 inside Single Match if.
                                    # We should probably define a stake input for daily schedule too.
                                    # For now, hardcode 1.00 or 100.00?
                                    # Let's use 100.00 as standard unit or just 1.0.
                                    stake = 100.0 
                                    s, m = place_bet(raw['game_id'], raw['side'], raw['odds'], stake, raw['edge'], raw['ev'])
                                    if s: st.toast(f"✅ Bet Placed on {raw['side']}!")
                                    else: st.toast(f"❌ Error: {m}")
                        
                        st.divider()

# --- TAB 2: BETS ---
with tab2:
    st.header("Betting Log")
    
    if st.button("Resolve Pending Bets"):
        n = resolve_bets()
        st.success(f"Resolved {n} bets.")
        
    conn = get_connection()
    df_bets = pd.read_sql("""
        SELECT b.id, b.placed_at, g.start_time, h.name as Home, a.name as Away, 
               b.side, b.odds, b.edge, b.stake, b.result, b.pnl
        FROM paper_bets b
        JOIN games g ON b.game_id = g.id
        JOIN teams h ON g.home_team_id = h.id
        JOIN teams a ON g.away_team_id = a.id
        ORDER BY b.placed_at DESC
    """, conn)
    conn.close()
    
    # Stats
    total_bets = len(df_bets)
    if total_bets > 0:
        profit = df_bets['pnl'].sum()
        wagered = df_bets['stake'].sum()
        roi = profit / wagered if wagered > 0 else 0
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Bets", total_bets)
        c2.metric("Total Profit", f"${profit:.2f}")
        c3.metric("ROI", f"{roi:.1%}")
    
    # Editable Dataframe
    edited_df = st.data_editor(
        df_bets,
        column_config={
            "id": st.column_config.NumberColumn(disabled=True),
            "placed_at": st.column_config.TextColumn(disabled=True),
            "start_time": st.column_config.TextColumn(disabled=True),
            "Home": st.column_config.TextColumn(disabled=True),
            "Away": st.column_config.TextColumn(disabled=True),
            "side": st.column_config.SelectboxColumn("Side", options=["HOME", "AWAY"]),
            "odds": st.column_config.NumberColumn("Odds", min_value=1.01, max_value=100.0, step=0.01),
            "edge": st.column_config.NumberColumn(disabled=True),
            "stake": st.column_config.NumberColumn("Stake", min_value=0.0, step=1.0),
            "result": st.column_config.SelectboxColumn("Result", options=["PENDING", "WIN", "LOSS", "PUSH"]),
            "pnl": st.column_config.NumberColumn("PnL")
        },
        hide_index=True,
        key="bets_editor"
    )

    if st.button("Save Changes"):
        conn = get_connection()
        cursor = conn.cursor()
        
        # We need to detect changes. 
        # Ideally, we compare edited_df with df_bets, or valid updates.
        # Logic: Iterate through edited_df and update each row by ID.
        # Optimization: Only update if changed? 
        # For simplicity in this interaction, we just update all rows present in edited_df.
        
        try:
            total_updated = 0
            for index, row in edited_df.iterrows():
                # We update editable fields: side, odds, stake, result, pnl
                # Note: edge is not editable here as it comes from snapshot
                bet_id = row['id']
                
                # Check original value to skip if no change? 
                # Doing naive update is safer for consistency unless list is huge.
                
                cursor.execute("""
                    UPDATE paper_bets
                    SET side = ?, odds = ?, stake = ?, result = ?, pnl = ?
                    WHERE id = ?
                """, (row['side'], row['odds'], row['stake'], row['result'], row['pnl'], bet_id))
                total_updated += 1
                
            conn.commit()
            st.success(f"✅ Successfully updated {total_updated} bets!")
            st.rerun()
            
        except Exception as e:
            st.error(f"Error saving changes: {e}")
        finally:
            conn.close()

# --- TAB 3: ADD GAME ---
with tab3:
    st.header("Add Game to Database")
    st.caption("Manually add upcoming games to the database for tracking.")
    
    with st.form("add_game_form"):
        f_date = st.date_input("Date")
        
        c1, c2 = st.columns(2)
        with c1:
            f_home = st.selectbox("Home Team", TEAM_NAMES, key="add_home")
            f_home_odds = st.number_input("Home Odds", min_value=1.01, max_value=100.0, value=1.90, step=0.01, key="add_h_odds")
        with c2:
            f_away = st.selectbox("Away Team", TEAM_NAMES, key="add_away", index=1)
            f_away_odds = st.number_input("Away Odds", min_value=1.01, max_value=100.0, value=1.90, step=0.01, key="add_a_odds")
        
        submitted = st.form_submit_button("Save Game")
        
        if submitted:
            if f_home == f_away:
                st.error("Teams must be different.")
            else:
                # Get IDs
                home_id = TEAMS_MAP.get(f_home)
                away_id = TEAMS_MAP.get(f_away)
                
                if not home_id or not away_id:
                    st.error("Error: Could not find Team IDs.")
                else:
                    success, msg = insert_game(f_date, home_id, away_id, f_home_odds, f_away_odds)
                    if success:
                        st.success(f"✅ {msg} ({f_home} vs {f_away})")
                    else:
                        st.error(f"❌ Failed: {msg}")

# --- TAB 4: DATA ---
with tab4:
    st.header("Game Data & Results")
    st.caption("View and Edit Game Results. Click 'Save Changes' to update the database.")
    
    # helper to fetch
    def load_data():
        conn = get_connection()
        # Join with new 'odds' table for Open/Close
        q = """
            SELECT g.id, g.start_time, h.name as home, a.name as away, 
                   g.home_score, g.away_score, g.status,
                   AVG(CASE WHEN o.snapshot_type = '10h' THEN o.home_odds END) as home_open,
                   AVG(CASE WHEN o.snapshot_type = '10h' THEN o.away_odds END) as away_open,
                   AVG(CASE WHEN o.snapshot_type = 'closing' THEN o.home_odds END) as home_close,
                   AVG(CASE WHEN o.snapshot_type = 'closing' THEN o.away_odds END) as away_close
            FROM games g
            JOIN teams h ON g.home_team_id = h.id
            JOIN teams a ON g.away_team_id = a.id
            LEFT JOIN odds o ON g.id = o.game_id
            GROUP BY g.id, g.start_time, h.name, a.name, g.home_score, g.away_score, g.status
            ORDER BY g.start_time DESC
            LIMIT 1000
        """
        d = pd.read_sql(q, conn)
        conn.close()
        return d

    df = load_data()
    
    # Editable Dataframe
    edited_df = st.data_editor(
        df,
        column_config={
            "id": st.column_config.NumberColumn(disabled=True),
            "start_time": st.column_config.DatetimeColumn(disabled=True, format="D MMM YYYY, HH:mm"),
            "home": st.column_config.TextColumn(disabled=True),
            "away": st.column_config.TextColumn(disabled=True),
            "home_score": st.column_config.NumberColumn("Home Score", min_value=0, step=1),
            "away_score": st.column_config.NumberColumn("Away Score", min_value=0, step=1),
            "status": st.column_config.SelectboxColumn("Status", options=["SCHEDULED", "FINAL", "LIVE", "POSTPONED"]),
            "home_open": st.column_config.NumberColumn("Home Open", format="%.2f", disabled=False),
            "away_open": st.column_config.NumberColumn("Away Open", format="%.2f", disabled=False),
            "home_close": st.column_config.NumberColumn("Home Close", format="%.2f", disabled=False),
            "away_close": st.column_config.NumberColumn("Away Close", format="%.2f", disabled=False),
        },
        disabled=["id", "start_time", "home", "away"],
        hide_index=True,
        use_container_width=True
    )
    
    if st.button("Save Changes", type="primary"):
        conn = get_connection()
        c = conn.cursor()
        updated_count = 0
        odds_updated_count = 0
        
        try:
            # We compare edited_df with strict original df (loaded before edit) could be tricky if sort changed.
            # But here sort is fixed by ID/Time in load_data.
            # We'll just Iterate and update efficiently.
            
            for i, row in edited_df.iterrows():
                # 1. Update Game Info (Score/Status)
                # Handle NaNs
                hs = row['home_score']
                as_ = row['away_score']
                if pd.isna(hs): hs = None
                if pd.isna(as_): as_ = None
                
                c.execute("""
                    UPDATE games 
                    SET home_score = ?, away_score = ?, status = ?
                    WHERE id = ?
                """, (hs, as_, row['status'], row['id']))
                updated_count += 1
                
                # 2. Update Odds
                # Helper to update/insert odds
                def update_odds_layer(gid, s_type, h_val, a_val):
                    if pd.isna(h_val) or pd.isna(a_val): return 0
                    
                    # Attempt Update first (updates ALL snapshots of this type for consistency)
                    c.execute("""
                        UPDATE odds 
                        SET home_odds = ?, away_odds = ?
                        WHERE game_id = ? AND snapshot_type = ?
                    """, (h_val, a_val, gid, s_type))
                    
                    if c.rowcount == 0:
                        # Insert if not exists
                        # Use 'Manual' as book to distinguish
                        c.execute("""
                            INSERT INTO odds (game_id, book, snapshot_type, home_odds, away_odds, snapshot_time)
                            VALUES (?, 'Manual', ?, ?, ?, datetime('now'))
                        """, (gid, s_type, h_val, a_val))
                        return 1
                    return 0

                # Check and Update Open
                update_odds_layer(row['id'], '10h', row['home_open'], row['away_open'])
                
                # Check and Update Close
                update_odds_layer(row['id'], 'closing', row['home_close'], row['away_close'])
                
            conn.commit()
            st.success(f"Saved {updated_count} game records and updated odds!")
            time.sleep(1) # feedback delay
            st.rerun() 
            
        except Exception as e:
            st.error(f"Error saving changes: {e}")
        finally:
            conn.close()
            
    st.divider()
    st.markdown("### Admin Actions")
    st.info("If you update game results, you must recalculate Elo ratings to reflect the changes.")
    
    if st.button("🔄 Recalculate All Elo Ratings"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            status_text.text("Fetching current ratings...")
            conn = get_connection()
            c = conn.cursor()
            
            # Store old ratings before reset
            c.execute("SELECT id, name, current_elo FROM teams ORDER BY name")
            old_ratings_data = c.fetchall()
            old_ratings = {row[0]: {'name': row[1], 'elo': row[2]} for row in old_ratings_data}
            
            status_text.text("Resetting ratings...")
            
            # 1. Reset all teams to starting Elo from config
            for team_name, starting_elo in TEAMS.items():
                c.execute("UPDATE teams SET current_elo = ? WHERE name = ?", (starting_elo, team_name))
            
            # 2. Fetch all FINAL games sorted by time
            c.execute("SELECT id, home_team_id, away_team_id, home_score, away_score FROM games WHERE status='FINAL' ORDER BY start_time ASC")
            games = c.fetchall()
            
            model = EloModel() 
            # Initialize with starting Elo from config
            c.execute("SELECT id, name FROM teams")
            teams_list = c.fetchall()
            model.ratings = {row[0]: TEAMS.get(row[1], 1500.0) for row in teams_list}
            
            total = len(games)
            status_text.text(f"Replaying {total} games...")
            
            for i, game in enumerate(games):
                gid, hid, aid, h_s, a_s = game
                
                h_elo = model.ratings.get(hid, 1500.0)
                a_elo = model.ratings.get(aid, 1500.0)
                
                # Update
                new_h, new_a = model.update_ratings(h_elo, a_elo, h_s > a_s, h_s, a_s)
                model.ratings[hid] = new_h
                model.ratings[aid] = new_a
                
                if i % 50 == 0:
                    progress_bar.progress((i + 1) / total)
            
            # 3. Save final ratings to DB
            status_text.text("Saving to database...")
            for tid, rating in model.ratings.items():
                c.execute("UPDATE teams SET current_elo = ? WHERE id = ?", (rating, tid))
                
            conn.commit()
            
            # 4. Fetch new ratings and display changes
            c.execute("SELECT id, name, current_elo FROM teams ORDER BY name")
            new_ratings_data = c.fetchall()
            
            conn.close()
            
            progress_bar.progress(1.0)
            status_text.text("Done!")
            st.success(f"Successfully recalculated ratings for {total} games!")
            
            # Display Elo changes
            st.subheader("📊 Elo Rating Changes")
            
            changes = []
            for row in new_ratings_data:
                tid, name, new_elo = row
                old_elo = old_ratings[tid]['elo']
                change = new_elo - old_elo
                changes.append({
                    'Team': name,
                    'Old Elo': f"{old_elo:.1f}",
                    'New Elo': f"{new_elo:.1f}",
                    'Change': f"{change:+.1f}"
                })
            
            df_changes = pd.DataFrame(changes)
            st.dataframe(df_changes, use_container_width=True, hide_index=True)
            
        except Exception as e:
            st.error(f"Error: {e}")
            import traceback
            st.code(traceback.format_exc())

