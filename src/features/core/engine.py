import asyncio
import logging
from datetime import datetime
from src.features.core.config import AMSTERDAM_TZ, PRICE_TRACKING_CONFIG
from src.features.loops.price_tracking import PriceTrackingLoop
from src.features.loops.peer_id_escalation import PeerIDEscalationLoop
from src.features.loops.trial_expiry import TrialExpiryLoop
from src.features.loops.preexpiration_warnings import PreexpirationWarningLoop
from src.features.loops.followup_dms import FollowupDMLoop
from src.features.loops.monday_activation import MondayActivationLoop
from src.features.loops.engagement_tracking import EngagementTrackingLoop
from src.features.loops.daily_trial_offers import DailyVIPTrialOfferLoop

logger = logging.getLogger(__name__)

class BackgroundEngine:
    """Orchestrates all 8 background loops for the trading bot"""
    
    def __init__(self, db, app):
        self.db = db
        self.app = app
        self.running = True
        self.loop_tasks = []
    
    async def start(self):
        """Start all 8 background loops and recovery features"""
        logger.info("Starting all background loops and recovery features...")
        
        try:
            # 1. Price Tracking Loop
            price_loop = PriceTrackingLoop(self.app, self.app)
            task1 = asyncio.create_task(price_loop.run())
            self.loop_tasks.append(task1)
            logger.info("✅ Price Tracking Loop started")
            
            # 2. Peer ID Escalation Loop (Handles all failed DMs via escalation)
            peer_loop = PeerIDEscalationLoop(self.app, self.db.pool, self.db)
            task2 = asyncio.create_task(peer_loop.run())
            self.loop_tasks.append(task2)
            logger.info("✅ Peer ID Escalation Loop started")
            
            # 3. Trial Expiry Loop
            trial_exp_loop = TrialExpiryLoop(self.app, self.db.pool, self.app)
            task3 = asyncio.create_task(trial_exp_loop.run())
            self.loop_tasks.append(task3)
            logger.info("✅ Trial Expiry Loop started")

            # 4. Pre-expiration Warning Loop
            warning_loop = PreexpirationWarningLoop(self.app, self.app)
            task4 = asyncio.create_task(warning_loop.run())
            self.loop_tasks.append(task4)
            logger.info("✅ Pre-expiration Warning Loop started")

            # 5. Follow-up DM Loop
            followup_loop = FollowupDMLoop(self.app, self.app)
            task5 = asyncio.create_task(followup_loop.run())
            self.loop_tasks.append(task5)
            logger.info("✅ Follow-up DM Loop started")
            
            # 6. Monday Activation Loop
            monday_loop = MondayActivationLoop(self.app, self.app)
            task6 = asyncio.create_task(monday_loop.run())
            self.loop_tasks.append(task6)
            logger.info("✅ Monday Activation Loop started")
            
            # 7. Engagement Tracking Loop
            engagement_loop = EngagementTrackingLoop(self.app, self.db.pool, self.app)
            task7 = asyncio.create_task(engagement_loop.run())
            self.loop_tasks.append(task7)
            logger.info("✅ Engagement Tracking Loop started")
            
            # 8. Daily Trial Offer Loop
            daily_loop = DailyVIPTrialOfferLoop(self.app, self.db.pool, self.app)
            task8 = asyncio.create_task(daily_loop.run())
            self.loop_tasks.append(task8)
            logger.info("✅ Daily Trial Offer Loop started")
            
            # FEATURE IMPLEMENTATIONS ON STARTUP
            # Feature 1: Missed Signal Recovery
            from src.features.trading.engine import TradingEngine
            trading_engine = TradingEngine(self.db.pool, self.app)
            asyncio.create_task(trading_engine.recover_missed_signals())
            
            # Feature 2: Trial Expiry Validation
            from src.features.community.trial_manager import TrialManager
            trial_manager = TrialManager(self.db.pool, self.app)
            asyncio.create_task(trial_manager.validate_and_fix_trial_expiry_times())
            
            # Feature 3: Offline Engagement Recovery
            asyncio.create_task(engagement_loop.recover_offline_engagement())
            
            # Feature 4: Active Trial Peer Initialization
            if hasattr(self.app, 'community_handlers'):
                asyncio.create_task(self.app.community_handlers.ensure_active_trial_peers())
            else:
                from src.features.community.handlers import CommunityHandlers
                self.app.community_handlers = CommunityHandlers(self.app, self.db.pool, self.app)
                asyncio.create_task(self.app.community_handlers.ensure_active_trial_peers())
            
            logger.info("🎉 All 8 background loops and recovery features successfully started!")
            
        except Exception as e:
            logger.error(f"❌ Error starting background loops: {e}")
            raise
    
    async def stop(self):
        """Stop all background loops"""
        logger.info("Stopping all background loops...")
        self.running = False
        
        for task in self.loop_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        logger.info("All background loops stopped.")
