SCHEMA = {
    "active_trades": """
        CREATE TABLE IF NOT EXISTS active_trades (
            message_id TEXT PRIMARY KEY,
            channel_id BIGINT,
            guild_id BIGINT,
            pair TEXT,
            action TEXT,
            entry_price FLOAT,
            tp1_price FLOAT,
            tp2_price FLOAT,
            tp3_price FLOAT,
            sl_price FLOAT,
            telegram_entry FLOAT,
            telegram_tp1 FLOAT,
            telegram_tp2 FLOAT,
            telegram_tp3 FLOAT,
            telegram_sl FLOAT,
            live_entry FLOAT,
            assigned_api TEXT,
            status TEXT DEFAULT 'active',
            tp_hits TEXT,
            breakeven_active BOOLEAN DEFAULT FALSE,
            entry_type TEXT,
            manual_overrides TEXT,
            channel_message_map TEXT,
            all_channel_ids TEXT,
            group_name TEXT,
            manual_tracking_only BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """,
    "completed_trades": """
        CREATE TABLE IF NOT EXISTS completed_trades (
            message_id TEXT PRIMARY KEY,
            channel_id BIGINT,
            guild_id BIGINT,
            pair TEXT,
            action TEXT,
            entry_price FLOAT,
            tp1_price FLOAT,
            tp2_price FLOAT,
            tp3_price FLOAT,
            sl_price FLOAT,
            telegram_entry FLOAT,
            telegram_tp1 FLOAT,
            telegram_tp2 FLOAT,
            telegram_tp3 FLOAT,
            telegram_sl FLOAT,
            live_entry FLOAT,
            assigned_api TEXT,
            final_status TEXT,
            tp_hits TEXT,
            breakeven_active BOOLEAN DEFAULT FALSE,
            entry_type TEXT,
            manual_overrides TEXT,
            created_at TIMESTAMP WITH TIME ZONE,
            completion_reason TEXT,
            archived_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """,
    "active_members": """
        CREATE TABLE IF NOT EXISTS active_members (
            member_id BIGINT PRIMARY KEY,
            role_added_time TIMESTAMP WITH TIME ZONE,
            role_id BIGINT,
            guild_id BIGINT,
            weekend_delayed BOOLEAN DEFAULT FALSE,
            expiry_time TIMESTAMP WITH TIME ZONE
        );
    """,
    "role_history": """
        CREATE TABLE IF NOT EXISTS role_history (
            member_id BIGINT PRIMARY KEY,
            first_granted TIMESTAMP WITH TIME ZONE,
            times_granted INTEGER DEFAULT 0,
            last_expired TIMESTAMP WITH TIME ZONE,
            guild_id BIGINT
        );
    """,
    "weekend_pending": """
        CREATE TABLE IF NOT EXISTS weekend_pending (
            member_id BIGINT PRIMARY KEY,
            join_time TIMESTAMP WITH TIME ZONE,
            guild_id BIGINT
        );
    """,
    "dm_schedule": """
        CREATE TABLE IF NOT EXISTS dm_schedule (
            member_id BIGINT PRIMARY KEY,
            role_expired TIMESTAMP WITH TIME ZONE,
            guild_id BIGINT,
            dm_3_sent BOOLEAN DEFAULT FALSE,
            dm_7_sent BOOLEAN DEFAULT FALSE,
            dm_14_sent BOOLEAN DEFAULT FALSE
        );
    """,
    "bot_status": """
        CREATE TABLE IF NOT EXISTS bot_status (
            id SERIAL PRIMARY KEY,
            last_online TIMESTAMP WITH TIME ZONE,
            heartbeat_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """,
    "peer_id_checks": """
        CREATE TABLE IF NOT EXISTS peer_id_checks (
            user_id BIGINT PRIMARY KEY,
            joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            peer_id_established BOOLEAN DEFAULT FALSE,
            established_at TIMESTAMP WITH TIME ZONE,
            current_delay_minutes INT DEFAULT 30,
            current_interval_minutes INT DEFAULT 3,
            last_check_at TIMESTAMP WITH TIME ZONE,
            next_check_at TIMESTAMP WITH TIME ZONE,
            welcome_dm_sent BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """,
    "emoji_reactions": """
        CREATE TABLE IF NOT EXISTS emoji_reactions (
            user_id BIGINT,
            message_id BIGINT,
            emoji TEXT,
            reaction_time TIMESTAMP WITH TIME ZONE,
            PRIMARY KEY (user_id, message_id, emoji)
        );
    """,
    "free_group_joins": """
        CREATE TABLE IF NOT EXISTS free_group_joins (
            user_id BIGINT PRIMARY KEY,
            joined_at TIMESTAMP WITH TIME ZONE NOT NULL,
            discount_sent BOOLEAN DEFAULT FALSE
        );
    """,
    "pending_welcome_dms": """
        CREATE TABLE IF NOT EXISTS pending_welcome_dms (
            user_id BIGINT PRIMARY KEY,
            first_name TEXT,
            message_type TEXT,
            message_content TEXT,
            failed_attempts INT DEFAULT 0,
            last_attempt TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """,
    "vip_trial_activations": """
        CREATE TABLE IF NOT EXISTS vip_trial_activations (
            user_id BIGINT PRIMARY KEY,
            activation_date TIMESTAMP WITH TIME ZONE NOT NULL,
            expiry_date TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """,
    "trial_offer_history": """
        CREATE TABLE IF NOT EXISTS trial_offer_history (
            user_id BIGINT PRIMARY KEY,
            offered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_trial_offer_user ON trial_offer_history(user_id);
    """,
    "missed_hits": """
        CREATE TABLE IF NOT EXISTS missed_hits (
            id SERIAL PRIMARY KEY,
            trade_id TEXT,
            pair TEXT,
            hit_type TEXT,
            price FLOAT,
            hit_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """
}
