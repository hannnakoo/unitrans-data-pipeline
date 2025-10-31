import requests
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import os
import asyncio
import aiohttp
import threading
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ScheduleBasedCollector:
    def __init__(self, db_config: Dict[str, str], route_tag: str, agency_tag: str = "unitrans"):
        """        
        Args:
            db_config: Dictionary with database connection parameters
            route_tag: Specific route to monitor
            agency_tag: Agency identifier for API calls
        """
        self.agency_tag = agency_tag
        self.route_tag = route_tag
        self.base_url = "https://retro.umoiq.com/service/publicXMLFeed"
        self.db_config = db_config
        self.conn = None
        
        # Route data
        self.stop_sequence = []  # Ordered list of stop tags
        self.schedule_trips = []  # List of trips with their scheduled times
        self.active_schedule = None  # (schedule_class, service_class)
        
        # Tracking
        self.location_polling_active = {}  # stop_tag -> Thread
        
    def connect_db(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def close_db(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def fetch_xml(self, command: str, **params) -> Optional[ET.Element]:
        """Fetch XML data from the API"""
        url_params = {
            'command': command,
            'a': self.agency_tag,
            **params
        }
        
        try:
            response = requests.get(self.base_url, params=url_params, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            return root
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {command}: {e}")
            return None
        except ET.ParseError as e:
            logger.error(f"XML parsing failed for {command}: {e}")
            return None
    
    def get_stop_sequence(self) -> List[str]:
        """
        Get the ordered sequence of stops from route config
        
        Returns:
            List of stop tags in order
        """
        logger.info(f"Fetching stop sequence for route {self.route_tag}...")
        
        root = self.fetch_xml('routeConfig', r=self.route_tag)
        if root is None:
            return []
        
        stops = []
        stop_tags_seen = set()
        
        for stop in root.findall('.//stop'):
            tag = stop.get('tag')
            stop_id = stop.get('stopId')
            
            # Skip arrival stops and duplicates
            if tag and stop_id and not tag.endswith('_ar') and tag not in stop_tags_seen:
                stops.append(tag)
                stop_tags_seen.add(tag)
        
        logger.info(f"Found {len(stops)} stops in sequence")
        return stops
    
    def get_current_schedule(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Determine the currently active schedule based on day of week
        
        Returns:
            Tuple of (schedule_class, service_class) or (None, None) if not found
        """
        logger.info(f"Determining active schedule for route {self.route_tag}...")
        
        root = self.fetch_xml('schedule', r=self.route_tag)
        if root is None:
            return None, None
        
        now = datetime.now()
        current_date = now.strftime('%Y%m%d')
        current_day = now.strftime('%A')
        
        # Find matching route elements
        for route_elem in root.findall('.//route'):
            schedule_class = route_elem.get('scheduleClass', '')
            service_class = route_elem.get('serviceClass', '')
            
            # Check if this service class matches current day
            match = False
            
            # Check for specific date (holiday schedule) - format: YYYYMMDD
            if service_class.isdigit() and len(service_class) == 8:
                if service_class == current_date:
                    match = True
                    logger.info(f"Matched holiday schedule for {current_date}")
            # Check for day patterns
            elif service_class == 'Saturday' and current_day == 'Saturday':
                match = True
            elif service_class == 'Sunday' and current_day == 'Sunday':
                match = True
            elif service_class == 'Wednesday' and current_day == 'Wednesday':
                match = True
            elif service_class == 'Friday' and current_day == 'Friday':
                match = True
            elif service_class == 'MoTuTh' and current_day in ['Monday', 'Tuesday', 'Thursday']:
                match = True
            
            if match:
                logger.info(f"Active schedule: {schedule_class} / {service_class}")
                return schedule_class, service_class
        
        logger.warning(f"No matching schedule found for {current_day}")
        return None, None
    
    def parse_schedule_trips(self, schedule_class: str, service_class: str) -> List[Dict]:
        """
        Parse schedule to get all trips with their stop times
        The first time corresponds to the first stop in route config, then loops through sequence
        
        Returns:
            List of dicts with trip info: {block_id, stops: {stop_tag: scheduled_time}, start_time}
        """
        logger.info(f"Parsing schedule trips for {schedule_class}/{service_class}...")
        
        root = self.fetch_xml('schedule', r=self.route_tag)
        if root is None:
            return []
        
        now = datetime.now()
        trips = []
        
        # Find the matching route element
        for route_elem in root.findall('.//route'):
            route_schedule = route_elem.get('scheduleClass', '')
            route_service = route_elem.get('serviceClass', '')
            
            if route_schedule == schedule_class and route_service == service_class:
                # Get header stops (for reference, though we use route config sequence)
                header_stops = []
                header = route_elem.find('header')
                if header is not None:
                    for stop in header.findall('stop'):
                        stop_tag = stop.get('tag')
                        if stop_tag:
                            header_stops.append(stop_tag)
                
                # Process all trips (tr elements)
                for tr in route_elem.findall('tr'):
                    block_id = tr.get('blockID', 'unknown')
                    
                    trip_stops = {}
                    first_time = None
                    
                    for stop in tr.findall('stop'):
                        stop_tag = stop.get('tag')
                        epoch_time = stop.get('epochTime')
                        time_str = stop.text
                        
                        if stop_tag and epoch_time and epoch_time != '-1' and time_str and time_str != '—':
                            try:
                                # Convert epoch time (milliseconds) to datetime
                                scheduled_dt = datetime.fromtimestamp(int(epoch_time) / 1000.0)
                                
                                # Adjust to today's date while keeping the time
                                scheduled_dt = datetime.combine(now.date(), scheduled_dt.time())
                                
                                # Handle times past midnight (early morning runs)
                                if scheduled_dt.hour < 3 and now.hour > 20:
                                    scheduled_dt += timedelta(days=1)
                                
                                trip_stops[stop_tag] = scheduled_dt
                                
                                if first_time is None:
                                    first_time = scheduled_dt
                                
                            except (ValueError, TypeError) as e:
                                logger.warning(f"Error parsing time for {stop_tag}: {e}")
                                continue
                    
                    if trip_stops and first_time:
                        trips.append({
                            'block_id': block_id,
                            'stops': trip_stops,
                            'start_time': first_time
                        })
        
        # Sort trips by start time
        trips.sort(key=lambda x: x['start_time'])
        
        logger.info(f"Found {len(trips)} trips in schedule")
        for i, trip in enumerate(trips[:5]):  # Log first 5 trips
            logger.info(f"  Trip {i+1}: Block {trip['block_id']}, starts {trip['start_time'].strftime('%H:%M:%S')}, {len(trip['stops'])} stops")
        
        return trips
    
    def should_move_to_next_stop(self, current_minutes: Optional[int], last_minutes: Optional[int]) -> bool:
        """
        Determine if we should move to the next stop based on prediction changes
        
        Args:
            current_minutes: Current prediction in minutes (None if no prediction)
            last_minutes: Previous prediction in minutes (None if first poll)
        
        Returns:
            True if should move to next stop
        """
        # No prediction available - bus may have passed or not coming
        if current_minutes is None:
            if last_minutes is not None:
                logger.info("Prediction disappeared, bus likely passed")
                return True
            return False
        
        # Prediction hit 0 minutes
        if current_minutes == 0:
            logger.info("Prediction at 0 minutes, moving to next stop")
            return True
        
        # Prediction increased (bus passed or reset)
        if last_minutes is not None and current_minutes > last_minutes + 2:
            logger.info(f"Prediction jumped from {last_minutes} to {current_minutes} minutes, bus likely passed")
            return True
        
        return False
    
    def get_current_stop(self, trip: Dict) -> Optional[str]:
        """
        Determine which stop the bus should currently be at or approaching
        
        Returns:
            Stop tag or None
        """
        now = datetime.now()
        
        # Find stops in order for this trip
        ordered_stops = []
        for stop_tag in self.stop_sequence:
            if stop_tag in trip['stops']:
                ordered_stops.append((stop_tag, trip['stops'][stop_tag]))
        
        if not ordered_stops:
            return None
        
        # Find the stop we should be tracking
        for i, (stop_tag, scheduled_time) in enumerate(ordered_stops):
            time_diff = (scheduled_time - now).total_seconds() / 60
            
            # Track stops within 10 minutes before to 5 minutes after scheduled time
            if -5 <= time_diff <= 10:
                return stop_tag
        
        # If we're past all stops, return None
        last_time = ordered_stops[-1][1]
        if now > last_time + timedelta(minutes=5):
            return None
        
        # Otherwise return first stop
        return ordered_stops[0][0]
    
    def poll_predictions(self, stop_tag: str) -> List[Tuple[str, str, int, datetime, datetime]]:
        """Poll prediction data for a single stop"""
        root = self.fetch_xml('predictions', r=self.route_tag, s=stop_tag)
        if root is None:
            return []
        
        poll_time = datetime.now()
        predictions = []
        
        for prediction in root.findall('.//prediction'):
            epoch_time = prediction.get('epochTime')
            minutes = prediction.get('minutes')
            
            if epoch_time and minutes:
                try:
                    predicted_time = datetime.fromtimestamp(int(epoch_time) / 1000.0)
                    predicted_minutes = int(minutes)
                    
                    predictions.append((
                        self.route_tag,
                        stop_tag,
                        predicted_minutes,
                        predicted_time,
                        poll_time
                    ))
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing prediction for {stop_tag}: {e}")
        
        return predictions
    
    def poll_vehicle_locations(self) -> List[Tuple[str, float, float, datetime]]:
        """Poll vehicle location data"""
        root = self.fetch_xml('vehicleLocations', r=self.route_tag, t=0)
        if root is None:
            return []
        
        poll_time = datetime.now()
        locations = []
        
        for vehicle in root.findall('.//vehicle'):
            lat = vehicle.get('lat')
            lon = vehicle.get('lon')
            route_tag = vehicle.get('routeTag')
            
            if lat and lon and route_tag:
                try:
                    locations.append((
                        route_tag,
                        float(lat),
                        float(lon),
                        poll_time
                    ))
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error parsing vehicle location: {e}")
        
        return locations
    
    def insert_predictions(self, predictions: List[Tuple[str, str, int, datetime, datetime]]):
        """Insert prediction data into the database"""
        if not predictions:
            return
        
        cursor = self.conn.cursor()
        try:
            execute_values(
                cursor,
                """INSERT INTO raw.predictions 
                   (route_id, stop_id, predicted_minutes, predicted_time, poll_time) 
                   VALUES %s""",
                predictions,
                template=None,
                page_size=1000
            )
            
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert predictions: {e}")
        finally:
            cursor.close()
    
    def insert_locations(self, locations: List[Tuple[str, float, float, datetime]]):
        """Insert vehicle location data into the database"""
        if not locations:
            return
        
        cursor = self.conn.cursor()
        try:
            execute_values(
                cursor,
                """INSERT INTO raw.locations 
                   (route_id, latitude, longitude, poll_time) 
                   VALUES %s""",
                locations,
                template=None,
                page_size=1000
            )
            
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert locations: {e}")
        finally:
            cursor.close()
    
    def start_location_polling(self, stop_tag: str, duration_seconds: int = 120):
        """
        Start a separate thread to poll locations every 15 seconds
        
        Args:
            stop_tag: Stop being monitored
            duration_seconds: How long to poll (default 2 minutes)
        """
        def poll_locations():
            logger.info(f"Starting high-frequency location polling for stop {stop_tag}")
            end_time = datetime.now() + timedelta(seconds=duration_seconds)
            
            while datetime.now() < end_time:
                locations = self.poll_vehicle_locations()
                if locations:
                    self.insert_locations(locations)
                    logger.debug(f"Polled {len(locations)} vehicle locations")
                
                asyncio.run(asyncio.sleep(15))
            
            logger.info(f"Finished location polling for stop {stop_tag}")
            del self.location_polling_active[stop_tag]
        
        # Start thread
        thread = threading.Thread(target=poll_locations, daemon=True)
        thread.start()
        self.location_polling_active[stop_tag] = thread
    
    def monitor_stop(self, stop_tag: str, scheduled_time: datetime) -> bool:
        """
        Monitor a single stop until it's time to move to the next one
        
        Returns:
            True if should continue to next stop, False if should end monitoring
        """
        logger.info(f"Monitoring stop {stop_tag} (scheduled: {scheduled_time.strftime('%H:%M:%S')})")
        
        location_thread_started = False
        last_prediction_minutes = None
        
        while True:
            now = datetime.now()
            
            # Check if we're past the stop window
            time_since_scheduled = (now - scheduled_time).total_seconds() / 60
            if time_since_scheduled > 10:
                logger.info(f"Stop {stop_tag} is more than 10 minutes past scheduled time, moving on")
                return True
            
            # Poll predictions
            predictions = self.poll_predictions(stop_tag)
            
            if predictions:
                min_prediction = min(p[2] for p in predictions)
                logger.info(f"Stop {stop_tag}: {min_prediction} minutes away")
                
                # Start location polling when within 1 minute
                if min_prediction <= 1 and not location_thread_started:
                    logger.info(f"Stop {stop_tag} within 1 minute, starting location polling")
                    self.start_location_polling(stop_tag, duration_seconds=180)
                    location_thread_started = True
                
                # Check if bus has passed (prediction increased significantly)
                if last_prediction_minutes is not None:
                    if min_prediction > last_prediction_minutes + 5:
                        logger.info(f"Stop {stop_tag}: prediction jumped from {last_prediction_minutes} to {min_prediction}, bus likely passed")
                        return True
                
                last_prediction_minutes = min_prediction
                
                # Store predictions
                self.insert_predictions(predictions)
            else:
                logger.debug(f"Stop {stop_tag}: no predictions available")
                
                # If we're past scheduled time and no predictions, bus likely passed
                if time_since_scheduled > 2:
                    logger.info(f"Stop {stop_tag}: no predictions and past scheduled time, moving on")
                    return True
            
            # Wait 60 seconds before next poll
            asyncio.run(asyncio.sleep(60))
    
    def run(self):
        """Main monitoring loop - continuously loops through route stops"""
        logger.info(f"Starting schedule-based collector for route {self.route_tag}")
        
        try:
            self.connect_db()
            
            # Load route configuration
            self.stop_sequence = self.get_stop_sequence()
            if not self.stop_sequence:
                logger.error("Failed to load stop sequence")
                return
            
            logger.info(f"Loaded {len(self.stop_sequence)} stops in sequence")
            logger.info(f"Route will loop continuously through these stops")
            
            # Start monitoring from first stop, loop indefinitely
            stop_index = 0
            loops_completed = 0
            
            while True:
                # Get current stop (wrap around if at end)
                current_stop_tag = self.stop_sequence[stop_index]
                
                # Monitor this stop
                should_continue = self.monitor_stop(current_stop_tag, stop_index)
                
                if not should_continue:
                    logger.info("Monitoring ended")
                    break
                
                # Move to next stop
                stop_index += 1
                
                # Check if we've completed a loop
                if stop_index >= len(self.stop_sequence):
                    loops_completed += 1
                    stop_index = 0  # Wrap around to beginning
                    logger.info(f"Completed loop {loops_completed}, starting over from first stop")
            
            logger.info("Monitoring completed")
            
        except KeyboardInterrupt:
            logger.info("Monitoring interrupted by user")
        except Exception as e:
            logger.error(f"Monitoring failed: {e}", exc_info=True)
            raise
        finally:
            # Wait for any active location polling threads
            for thread in self.location_polling_active.values():
                thread.join(timeout=5)
            
            self.close_db()


def main():    
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'unitrans_db'),
        'user': os.getenv('DB_USER', 'unitrans_user'),
        'password': os.getenv('DB_PASSWORD', 'unitrans_pass'),
        'port': os.getenv('DB_PORT', 5432)
    }
    
    # Route to monitor - can be passed as argument or environment variable
    route_tag = os.getenv('ROUTE_TAG')
    if not route_tag:
        import sys
        if len(sys.argv) > 1:
            route_tag = sys.argv[1]
        else:
            route_tag = 'A'  # Default
    
    logger.info(f"Monitoring route: {route_tag}")
    
    collector = ScheduleBasedCollector(db_config, route_tag)
    collector.run()


if __name__ == "__main__":
    main()