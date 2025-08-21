import requests
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from typing import List, Dict, Tuple
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransitDataInitializer:
    def __init__(self, db_config: Dict[str, str], agency_tag: str = "unitrans"):
        """        
        Args:
            db_config: Dictionary with database connection parameters
            agency_tag: Agency identifier for API calls
        """
        self.agency_tag = agency_tag
        self.base_url = "https://retro.umoiq.com/service/publicXMLFeed"
        self.db_config = db_config
        self.conn = None
        
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
    
    def fetch_xml(self, command: str, **params) -> ET.Element:
        url_params = {
            'command': command,
            'a': self.agency_tag,
            **params
        }
        
        try:
            response = requests.get(self.base_url, params=url_params, timeout=30)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            logger.info(f"Successfully fetched {command} data")
            return root
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {command}: {e}")
            raise
        except ET.ParseError as e:
            logger.error(f"XML parsing failed for {command}: {e}")
            raise
    
    def get_routes(self) -> List[str]:
        """        
        Returns:
            List of tuples (route_tag, route_title)
        """
        logger.info("Fetching route list...")
        root = self.fetch_xml('routeList')

        routes = []
        for route in root.findall('.//route'):
            tag = route.get('tag')
            if tag:
                routes.append(tag)
        
        logger.info(f"Found {len(routes)} routes")
        return routes
    
    def get_stops_for_route(self, route_tag: str) -> List[Tuple[str, str, float, float, str]]:
        """                    
        Returns:
            List of tuples (stop_tag, stop_title, lat, lon, stop_id)
        """
        logger.info(f"Fetching stops for route {route_tag}...")
        root = self.fetch_xml('routeConfig', r=route_tag)        
        
        stops = []
        stop_tag_id = dict()
        arrival_stops = list()
        for stop in root.findall('.//stop'):
            tag = stop.get('tag')
            title = stop.get('title')
            lat = stop.get('lat')
            lon = stop.get('lon')
            stop_id = stop.get('stopId')
            if tag[-3:] == "_ar": # Save arrival stops for later processing
                if tag and title and lat and lon:
                    arrival_stops.append((tag, title, float(lat), float(lon)))
                continue
            
            if tag and title and lat and lon:
                try:
                    stops.append((tag, title, float(lat), float(lon), stop_id))
                    stop_tag_id[tag] = stop_id
                except ValueError:
                    logger.warning(f"Invalid coordinates for stop {tag}: lat={lat}, lon={lon}")
                    continue

        for stop in arrival_stops:
            tag, title, lat, lon = stop
            stop_id = stop_tag_id.get(tag[:-3])
            stops.append((tag, title, lat, lon, stop_id))
            logger.info(f"Added arrival stop {tag} with ID {stop_id}")
        
        logger.info(f"Found {len(stops)} stops for route {route_tag}")
        return stops
    
    def get_schedule_for_route(self, route_tag: str) -> List[Tuple[str, str, str, str, str]]:
        """                    
        Returns:
            List of tuples (route_id, stop_id, schedule_class, service_class, scheduled_time)
        """
        logger.info(f"Fetching schedule for route {route_tag}...")
        root = self.fetch_xml('schedule', r=route_tag)
        
        schedule_data = []
        
        route_elements = root.findall('.//route')
        if not route_elements:
            logger.warning(f"No schedule data found for route {route_tag}")
            return []
        
        logger.info(f"Found {len(route_elements)} route schedule variants for route {route_tag}")
        
        # Process each route element separately
        for route_idx, route_elem in enumerate(route_elements):
            route_actual_tag = route_elem.get('tag', route_tag)
            schedule_class = route_elem.get('scheduleClass', 'unknown')
            service_class = route_elem.get('serviceClass', 'unknown')
            direction = route_elem.get('direction', 'unknown')

            # if service_class.find('Service') != -1:
            #     logger.warning(f"Skipping route with service class {service_class} as it is not a valid service route")
            #     continue

            logger.info(f"Processing route variant {route_idx + 1}: tag={route_actual_tag}, "
                       f"scheduleClass={schedule_class}, serviceClass={service_class}, direction={direction}")
            
            # Process each trip (tr element) within this route element
            trips = route_elem.findall('tr')
            logger.info(f"Found {len(trips)} trips for route variant {route_idx + 1}")
            
            for tr in trips:                
                for stop in tr.findall('stop'):
                    stop_tag = stop.get('tag')
                    epoch_time = stop.get('epochTime')
                    time_content = stop.text
                    
                    if stop_tag and epoch_time and time_content:
                        try:
                            scheduled_time = time_content
                            
                            schedule_data.append((
                                route_actual_tag,
                                stop_tag,
                                schedule_class,
                                service_class,
                                scheduled_time
                            ))
                        except (ValueError, TypeError):
                            logger.warning(f"Error processing: route={route_actual_tag}, stop={stop_tag}, schedule class={schedule_class}, "
                            f"service class={service_class}, time={time_content}")
                            continue
        
        logger.info(f"Found {len(schedule_data)} total schedule entries for route {route_tag}")
        return schedule_data
    
    def populate_routes(self, routes: List[str]):
        """Insert routes into the database"""
        logger.info("Populating routes table...")
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE core.routes CASCADE")
            
            execute_values(
                cursor,
                "INSERT INTO core.routes (id) VALUES %s ON CONFLICT (id) DO NOTHING",
                routes,
                template=None,
                page_size=100
            )
            
            self.conn.commit()
            logger.info(f"Inserted {len(routes)} routes")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to populate routes: {e}")
            raise
        finally:
            cursor.close()
    
    def populate_stops(self, all_stops: List[Tuple[str, str, float, float, str]]):
        """Insert stops into the database"""
        logger.info("Populating stops table...")
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE core.stops CASCADE")
            
            unique_stops = {} # Make sure stop tag is unique
            for stop_tag, title, lat, lon, stop_id in all_stops:
                if stop_tag not in unique_stops:
                    unique_stops[stop_tag] = (stop_tag, title, lat, lon, stop_id)
                else:
                    logger.warning(f"Dropped duplicate stop: ({stop_tag}, {title}, {lat}, {lon}, {stop_id})")
            
            stops_data = list(unique_stops.values())
            
            execute_values(
                cursor,
                """INSERT INTO core.stops (id, name, latitude, longitude, tag) 
                   VALUES %s ON CONFLICT (id) DO UPDATE SET 
                   name = EXCLUDED.name, 
                   latitude = EXCLUDED.latitude, 
                   longitude = EXCLUDED.longitude, 
                   tag = EXCLUDED.tag""",
                stops_data,
                template=None,
                page_size=100
            )
            
            self.conn.commit()
            logger.info(f"Inserted {len(stops_data)} unique stops")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to populate stops: {e}")
            raise
        finally:
            cursor.close()
    
    def populate_schedule(self, all_schedule_data: List[Tuple[str, str, str, str, str]]):
        """Insert schedule data into the database"""
        logger.info("Populating route_stop_times table...")
        
        cursor = self.conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE core.route_stop_times CASCADE")

            unique_sched = set(all_schedule_data)
            if not unique_sched:
                logger.warning("No schedule data to insert")
            if len(unique_sched) != len(all_schedule_data):
                logger.warning(f"Found {len(all_schedule_data) - len(unique_sched)} duplicate schedule entries, removing duplicates")
            sched_data = list(unique_sched)
            
            execute_values(
                cursor,
                """INSERT INTO core.route_stop_times
                    (route_id, stop_id, schedule_class, service_class, scheduled_time) 
                    VALUES %s""",
                sched_data,
                template=None,
                page_size=1000
            )
            
            self.conn.commit()
            logger.info(f"Inserted {len(all_schedule_data)} schedule entries")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to populate schedule: {e}")
            raise
        finally:
            cursor.close()
    
    def initialize_database(self, max_routes: int = None):
        logger.info(f"Starting database initialization for agency: {self.agency_tag}")
        
        try:
            self.connect_db()
            
            routes = self.get_routes()
            if max_routes:
                routes = routes[:max_routes]
                logger.info(f"Limited to {max_routes} routes for testing")
            
            self.populate_routes(routes)
            
            all_stops = []
            all_schedule_data = []
            
            for i, route_tag in enumerate(routes, 1):
                logger.info(f"Processing route {i}/{len(routes)}: {route_tag}")
                
                try:
                    route_stops = self.get_stops_for_route(route_tag)
                    all_stops.extend(route_stops)
                    
                    route_schedule = self.get_schedule_for_route(route_tag)
                    all_schedule_data.extend(route_schedule)
                    
                except Exception as e:
                    logger.error(f"Failed to process route {route_tag}: {e}")
                    continue
            
            self.populate_stops(all_stops)
            
            self.populate_schedule(all_schedule_data)
            
            logger.info("Database initialization completed successfully!")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
        finally:
            self.close_db()


def main():    
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'unitrans_db'),
        'user': os.getenv('DB_USER', 'unitrans_user'),
        'password': os.getenv('DB_PASSWORD', 'unitrans_pass'),
        'port': os.getenv('DB_PORT', 5432)
    }
    
    initializer = TransitDataInitializer(db_config)

    initializer.initialize_database(max_routes=1)


if __name__ == "__main__":
    main()