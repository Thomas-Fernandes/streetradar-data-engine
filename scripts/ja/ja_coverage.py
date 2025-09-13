#!/usr/bin/env python3
"""
JA360 Coverage Generator
Snaps JA360 panoramas to OSM road network and generates coverage LineStrings

This script implements the algorithm to:
1. Load JA360 panoramas and OSM road network
2. Snap each panorama to the closest OSM edge using distance + heading
3. Generate coverage intervals along roads
4. Create LineString segments for covered portions
5. Export snapped points and coverage lines

Input:
- JA360 panoramas GeoJSON (from ja_processor.py)
- OSM road network (PBF file or preprocessed GPKG)

Output:
- panos_snapped.parquet: snapped panorama assignments
- coverage_lines.gpkg: covered road segments
- coverage_lines.geojson: same as above in GeoJSON format
"""

import os
import sys
import time
import math
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict
import warnings

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, MultiLineString
from shapely.ops import substring, unary_union
from shapely.strtree import STRtree
import pyproj
from pyproj import CRS, Transformer

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)


@dataclass
class SnapConfig:
    """Configuration parameters for snapping algorithm"""
    # Spatial search parameters
    r_query: float = 25.0          # Search radius for candidates (m)
    d_max: float = 30.0            # Maximum allowed snap distance (m)
    theta_max: float = 135.0       # Maximum heading deviation (degrees)
    
    # Scoring weights
    w_distance: float = 0.7        # Weight for distance component
    w_heading: float = 0.3         # Weight for heading component
    
    # Distance/heading normalization caps
    d_cap: float = 15.0            # Distance normalization cap (m)
    theta_cap: float = 90.0        # Heading normalization cap (degrees)
    
    # Local bearing calculation
    epsilon: float = 5.0           # Window for local bearing calculation (m)
    
    # Coverage parameters
    coverage_radius: float = 12.0  # Default coverage radius (m)
    r_min: float = 8.0            # Minimum coverage radius (m)
    gap_merge: float = 5.0        # Gap threshold for merging intervals (m)
    min_segment_length: float = 3.0 # Minimum segment length to keep (m)
    
    # OSM highway types to include
    highway_types: List[str] = None
    
    def __post_init__(self):
        if self.highway_types is None:
            self.highway_types = [
                'motorway', 'trunk', 'primary', 'secondary', 'tertiary',
                'unclassified', 'residential', 'living_street', 'service', 'track'
            ]


class JA360CoverageGenerator:
    """Main class for generating JA360 coverage from panorama data"""
    
    def __init__(self, config: SnapConfig = None):
        self.config = config or SnapConfig()
        self.logger = self._setup_logging()
        
        # Data containers
        self.edges_gdf = None
        self.panos_gdf = None
        self.assignments = []
        self.intervals_by_edge = defaultdict(list)
        
        # CRS management
        self.source_crs = 'EPSG:4326'  # WGS84
        self.target_crs = 'EPSG:3857'  # Web Mercator (meters)
        self.transformer_to_metric = None
        self.transformer_to_wgs84 = None
        
        # Spatial index
        self.edges_tree = None
        self.geom_to_edge_id = {}
        
        self.logger.info("🗺️  JA360 Coverage Generator initialized")
        self.logger.info(f"📐 Search radius: {self.config.r_query}m")
        self.logger.info(f"📏 Max snap distance: {self.config.d_max}m")
        self.logger.info(f"🧭 Max heading deviation: {self.config.theta_max}°")
        self.logger.info(f"📊 Coverage radius: {self.config.coverage_radius}m")
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('JA360Coverage')
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        return logger
    
    def _setup_crs_transformers(self):
        """Setup CRS transformers for coordinate conversion"""
        # Create transformers
        self.transformer_to_metric = Transformer.from_crs(
            self.source_crs, self.target_crs, always_xy=True
        )
        self.transformer_to_wgs84 = Transformer.from_crs(
            self.target_crs, self.source_crs, always_xy=True
        )
        
        self.logger.info(f"🌐 CRS transformers: {self.source_crs} ↔ {self.target_crs}")
    
    def load_osm_network(self, osm_file: Union[str, Path]) -> gpd.GeoDataFrame:
        """
        Load and prepare OSM road network
        
        Args:
            osm_file: Path to OSM PBF file or preprocessed GPKG
            
        Returns:
            GeoDataFrame with prepared road network
        """
        osm_path = Path(osm_file)
        self.logger.info(f"🛣️  Loading OSM network from {osm_path}")
        
        if not osm_path.exists():
            raise FileNotFoundError(f"OSM file not found: {osm_path}")
        
        start_time = time.time()
        
        # Load based on file extension
        if osm_path.suffix.lower() in ['.gpkg', '.shp']:
            # Load from vector format
            gdf = gpd.read_file(osm_path)
            self.logger.info(f"📊 Loaded {len(gdf):,} features from vector file")
        
        elif osm_path.suffix.lower() == '.pbf':
            # Load from PBF using OSMnx or similar
            try:
                import osmium
                self.logger.error("❌ PBF loading not implemented yet")
                self.logger.error("💡 Please convert PBF to GPKG first using GDAL:")
                self.logger.error(f"   ogr2ogr -f GPKG {osm_path.with_suffix('.gpkg')} {osm_path} lines")
                raise NotImplementedError("PBF loading requires additional implementation")
            except ImportError:
                self.logger.error("❌ osmium package required for PBF files")
                raise
        
        else:
            raise ValueError(f"Unsupported file format: {osm_path.suffix}")
        
        # Ensure we have required columns
        required_cols = ['geometry']
        optional_cols = ['highway', 'name', 'ref', 'oneway', 'osm_id', 'way_id']
        
        for col in required_cols:
            if col not in gdf.columns:
                raise ValueError(f"Required column '{col}' not found in OSM data")
        
        # Add missing optional columns
        for col in optional_cols:
            if col not in gdf.columns:
                gdf[col] = None
        
        # Use osm_id as way_id if way_id not present
        if 'way_id' not in gdf.columns and 'osm_id' in gdf.columns:
            gdf['way_id'] = gdf['osm_id']
        
        # Filter by highway type
        if 'highway' in gdf.columns:
            before_filter = len(gdf)
            gdf = gdf[gdf['highway'].isin(self.config.highway_types)].copy()
            self.logger.info(f"🚦 Filtered to {len(gdf):,} roads (from {before_filter:,})")
        
        # Explode MultiLineStrings to LineStrings
        if any(gdf.geometry.geom_type == 'MultiLineString'):
            self.logger.info("💥 Exploding MultiLineStrings...")
            gdf = gdf.explode(index_parts=False).reset_index(drop=True)
        
        # Filter to LineStrings only
        gdf = gdf[gdf.geometry.geom_type == 'LineString'].copy()
        self.logger.info(f"📏 {len(gdf):,} LineString features retained")
        
        # Add edge_id
        gdf['edge_id'] = range(len(gdf))
        
        # Transform to metric CRS
        if gdf.crs != self.target_crs:
            gdf = gdf.to_crs(self.target_crs)
        
        # Calculate length
        gdf['length_m'] = gdf.geometry.length
        
        # Remove zero-length edges
        gdf = gdf[gdf['length_m'] > 0].reset_index(drop=True)
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ OSM network prepared in {elapsed:.1f}s")
        self.logger.info(f"📊 Final dataset: {len(gdf):,} edges")
        
        return gdf
    
    def load_panoramas(self, panos_file: Union[str, Path]) -> gpd.GeoDataFrame:
        """
        Load JA360 panorama data
        
        Args:
            panos_file: Path to GeoJSON file with panoramas
            
        Returns:
            GeoDataFrame with panorama data
        """
        panos_path = Path(panos_file)
        self.logger.info(f"📷 Loading panoramas from {panos_path}")
        
        if not panos_path.exists():
            raise FileNotFoundError(f"Panoramas file not found: {panos_path}")
        
        start_time = time.time()
        
        # Load panoramas
        gdf = gpd.read_file(panos_path)
        self.logger.info(f"📊 Loaded {len(gdf):,} panoramas")
        
        # Ensure we have required columns
        if 'geometry' not in gdf.columns:
            raise ValueError("Missing 'geometry' column in panoramas")
        
        # Filter to Points only
        gdf = gdf[gdf.geometry.geom_type == 'Point'].copy()
        
        # Ensure pano_id column
        if 'pano_id' not in gdf.columns:
            if 'id' in gdf.columns:
                gdf['pano_id'] = gdf['id']
            else:
                gdf['pano_id'] = range(len(gdf))
        
        # Transform to metric CRS
        if gdf.crs != self.target_crs:
            gdf = gdf.to_crs(self.target_crs)
        
        # Check for heading column
        has_heading = 'heading' in gdf.columns
        if has_heading:
            # Clean heading values
            gdf['heading'] = pd.to_numeric(gdf['heading'], errors='coerce')
            valid_headings = gdf['heading'].notna().sum()
            self.logger.info(f"🧭 {valid_headings:,} panoramas have heading data")
        else:
            self.logger.info("🧭 No heading data available")
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ Panoramas loaded in {elapsed:.1f}s")
        
        return gdf
    
    def build_spatial_index(self):
        """Build spatial index for efficient edge lookup"""
        self.logger.info("🗂️  Building spatial index...")
        start_time = time.time()
        
        # Create STRtree
        geoms = self.edges_gdf.geometry.values
        self.edges_tree = STRtree(geoms)
        
        # Create geometry to edge_id mapping
        self.geom_to_edge_id = {
            geom: edge_id for geom, edge_id in zip(geoms, self.edges_gdf['edge_id'])
        }
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ Spatial index built in {elapsed:.1f}s")
    
    def calculate_bearing(self, p1: Point, p2: Point) -> float:
        """
        Calculate bearing from p1 to p2 in degrees (0-360)
        
        Args:
            p1, p2: Points in metric coordinates
            
        Returns:
            Bearing in degrees [0, 360)
        """
        dx = p2.x - p1.x
        dy = p2.y - p1.y
        
        bearing = math.degrees(math.atan2(dx, dy))
        return (bearing + 360) % 360
    
    def circular_diff(self, angle1: float, angle2: float) -> float:
        """
        Calculate circular difference between two angles
        
        Returns:
            Difference in degrees [0, 180]
        """
        diff = abs(angle1 - angle2)
        return min(diff, 360 - diff)
    
    def get_local_bearing(self, edge: LineString, s: float) -> Optional[float]:
        """
        Calculate local bearing of edge at position s
        
        Args:
            edge: LineString geometry
            s: Position along edge (meters)
            
        Returns:
            Local bearing in degrees or None if calculation fails
        """
        try:
            length = edge.length
            epsilon = min(self.config.epsilon, length / 4)  # Adaptive epsilon
            
            s1 = max(0, s - epsilon)
            s2 = min(length, s + epsilon)
            
            p1 = edge.interpolate(s1)
            p2 = edge.interpolate(s2)
            
            if p1.equals(p2):
                return None
                
            return self.calculate_bearing(p1, p2)
        
        except Exception:
            return None
    
    def snap_panorama(self, pano_point: Point, pano_heading: Optional[float] = None) -> Optional[Dict]:
        """
        Snap a panorama to the best matching edge
        
        Args:
            pano_point: Panorama position (metric coordinates)
            pano_heading: Panorama heading in degrees (optional)
            
        Returns:
            Dict with snap results or None if no valid snap found
        """
        # Query candidates within search radius
        query_buffer = pano_point.buffer(self.config.r_query)
        candidate_indices = self.edges_tree.query(query_buffer)
        
        if len(candidate_indices) == 0:
            return None
        
        best_match = None
        best_score = float('inf')
        
        for idx in candidate_indices:
            try:
                edge_geom = self.edges_gdf.iloc[idx].geometry
                edge_id = self.edges_gdf.iloc[idx].edge_id
                edge_length = self.edges_gdf.iloc[idx].length_m
                
                # Calculate distance
                distance = pano_point.distance(edge_geom)
                if distance > self.config.d_max:
                    continue
                
                # Project point onto edge
                s = edge_geom.project(pano_point)
                projected_point = edge_geom.interpolate(s)
                
                # Calculate score components
                d_norm = min(distance, self.config.d_cap) / self.config.d_cap
                
                # Heading component (if available)
                if pano_heading is not None and not np.isnan(pano_heading):
                    local_bearing = self.get_local_bearing(edge_geom, s)
                    if local_bearing is not None:
                        dtheta = self.circular_diff(local_bearing, pano_heading)
                        if dtheta > self.config.theta_max:
                            continue  # Skip if heading deviation too large
                        theta_norm = min(dtheta, self.config.theta_cap) / self.config.theta_cap
                    else:
                        theta_norm = 1.0  # Penalize if can't calculate bearing
                        dtheta = 0.0
                else:
                    theta_norm = 0.0  # No heading penalty
                    dtheta = 0.0
                
                # Combined score
                if pano_heading is not None and not np.isnan(pano_heading):
                    score = self.config.w_distance * d_norm + self.config.w_heading * theta_norm
                else:
                    score = d_norm  # Only distance-based scoring
                
                if score < best_score:
                    best_score = score
                    best_match = {
                        'edge_id': edge_id,
                        's': s,
                        'edge_length': edge_length,
                        'snap_distance': distance,
                        'dtheta': dtheta,
                        'score': score,
                        'projected_point': projected_point
                    }
            
            except Exception as e:
                continue
        
        return best_match
    
    def assign_match_quality(self, snap_dist: float, dtheta: float) -> str:
        """
        Assign match quality grade based on snap distance and heading deviation
        
        Args:
            snap_dist: Snap distance in meters
            dtheta: Heading deviation in degrees
            
        Returns:
            Quality grade: 'A', 'B', 'C', or 'D'
        """
        # Distance thresholds
        if snap_dist <= 5:
            dist_grade = 'A'
        elif snap_dist <= 10:
            dist_grade = 'B' 
        elif snap_dist <= 20:
            dist_grade = 'C'
        else:
            dist_grade = 'D'
        
        # Heading thresholds  
        if dtheta <= 15:
            heading_grade = 'A'
        elif dtheta <= 30:
            heading_grade = 'B'
        elif dtheta <= 60:
            heading_grade = 'C'
        else:
            heading_grade = 'D'
        
        # Overall grade is the worse of the two
        grades = ['A', 'B', 'C', 'D']
        dist_idx = grades.index(dist_grade)
        heading_idx = grades.index(heading_grade)
        
        return grades[max(dist_idx, heading_idx)]
    
    def snap_all_panoramas(self):
        """Snap all panoramas to road network"""
        self.logger.info("📍 Starting panorama snapping...")
        start_time = time.time()
        
        self.assignments = []
        successful_snaps = 0
        
        # Process panoramas in batches for progress reporting
        batch_size = 10000
        total_panos = len(self.panos_gdf)
        
        for i in range(0, total_panos, batch_size):
            batch_end = min(i + batch_size, total_panos)
            batch = self.panos_gdf.iloc[i:batch_end]
            
            for idx, row in batch.iterrows():
                pano_point = row.geometry
                pano_id = row.pano_id
                pano_heading = row.get('heading', None)
                
                # Handle NaN headings
                if pano_heading is not None and pd.isna(pano_heading):
                    pano_heading = None
                
                # Attempt snap
                snap_result = self.snap_panorama(pano_point, pano_heading)
                
                if snap_result:
                    # Determine quality
                    quality = self.assign_match_quality(
                        snap_result['snap_distance'], 
                        snap_result['dtheta']
                    )
                    
                    assignment = {
                        'pano_id': pano_id,
                        'edge_id': snap_result['edge_id'],
                        's': snap_result['s'],
                        'edge_length': snap_result['edge_length'],
                        'snap_distance': snap_result['snap_distance'],
                        'dtheta': snap_result['dtheta'],
                        'score': snap_result['score'],
                        'match_quality': quality
                    }
                    
                    self.assignments.append(assignment)
                    successful_snaps += 1
            
            # Progress report
            if i + batch_size >= total_panos or (i + batch_size) % 50000 == 0:
                progress = min(batch_end, total_panos)
                elapsed = time.time() - start_time
                rate = progress / elapsed if elapsed > 0 else 0
                success_rate = successful_snaps / progress * 100
                
                self.logger.info(f"📊 {progress:,}/{total_panos:,} ({progress/total_panos*100:.1f}%) - "
                               f"{rate:.1f} panos/s - {success_rate:.1f}% success rate")
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ Snapping completed in {elapsed:.1f}s")
        self.logger.info(f"📊 {successful_snaps:,}/{total_panos:,} panoramas snapped ({successful_snaps/total_panos*100:.1f}%)")
        
        # Quality distribution
        if self.assignments:
            df = pd.DataFrame(self.assignments)
            quality_dist = df['match_quality'].value_counts().sort_index()
            self.logger.info("🏆 Quality distribution:")
            for grade, count in quality_dist.items():
                self.logger.info(f"   {grade}: {count:,} ({count/len(df)*100:.1f}%)")
    
    def calculate_coverage_intervals(self):
        """Calculate coverage intervals for each edge"""
        self.logger.info("📏 Calculating coverage intervals...")
        start_time = time.time()
        
        # Group assignments by edge
        df = pd.DataFrame(self.assignments)
        
        # Optional: filter by quality
        # df = df[df['match_quality'].isin(['A', 'B', 'C'])].copy()
        
        for edge_id, group in df.groupby('edge_id'):
            edge_length = group.iloc[0]['edge_length']
            
            # Get all s positions for this edge
            s_positions = group['s'].values
            
            # Calculate adaptive radius if desired
            if len(s_positions) > 2:
                deltas = np.diff(np.sort(s_positions))
                median_delta = np.median(deltas)
                adaptive_radius = max(self.config.r_min, 0.5 * median_delta)
                radius = min(self.config.coverage_radius, adaptive_radius)
            else:
                radius = self.config.coverage_radius
            
            # Create intervals
            intervals = []
            for s in s_positions:
                start = max(0, s - radius)
                end = min(edge_length, s + radius)
                intervals.append((start, end))
            
            # Sort and merge overlapping intervals
            intervals.sort()
            merged = []
            
            for start, end in intervals:
                if not merged or start > merged[-1][1] + self.config.gap_merge:
                    merged.append((start, end))
                else:
                    # Merge with previous interval
                    merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            
            # Store merged intervals with point counts
            final_intervals = []
            for start, end in merged:
                # Count points in this interval
                points_in_interval = np.sum(
                    (s_positions >= start - radius) & (s_positions <= end + radius)
                )
                final_intervals.append((start, end, points_in_interval))
            
            self.intervals_by_edge[edge_id] = final_intervals
        
        elapsed = time.time() - start_time
        total_intervals = sum(len(intervals) for intervals in self.intervals_by_edge.values())
        self.logger.info(f"✅ Coverage intervals calculated in {elapsed:.1f}s")
        self.logger.info(f"📊 {total_intervals:,} intervals on {len(self.intervals_by_edge):,} edges")
    
    def create_coverage_segments(self) -> gpd.GeoDataFrame:
        """Create coverage segments from intervals"""
        self.logger.info("✂️  Creating coverage segments...")
        start_time = time.time()
        
        segments = []
        
        for edge_id, intervals in self.intervals_by_edge.items():
            # Get edge info
            edge_row = self.edges_gdf[self.edges_gdf.edge_id == edge_id].iloc[0]
            edge_geom = edge_row.geometry
            
            for start_s, end_s, n_points in intervals:
                # Skip very short segments
                segment_length = end_s - start_s
                if segment_length < self.config.min_segment_length:
                    continue
                
                try:
                    # Create segment geometry using substring
                    segment_geom = substring(edge_geom, start_s, end_s, normalized=False)
                    
                    # Skip if segment creation failed
                    if segment_geom is None or segment_geom.is_empty:
                        continue
                    
                    segment = {
                        'edge_id': edge_id,
                        'way_id': edge_row.get('way_id'),
                        'highway': edge_row.get('highway'),
                        'name': edge_row.get('name'),
                        'ref': edge_row.get('ref'),
                        'oneway': edge_row.get('oneway'),
                        'start_s': start_s,
                        'end_s': end_s,
                        'segment_length': segment_length,
                        'n_points': n_points,
                        'density': n_points / segment_length if segment_length > 0 else 0,
                        'geometry': segment_geom
                    }
                    
                    segments.append(segment)
                
                except Exception as e:
                    continue
        
        # Create GeoDataFrame
        if segments:
            segments_gdf = gpd.GeoDataFrame(segments, crs=self.target_crs)
        else:
            # Create empty GeoDataFrame with correct schema
            segments_gdf = gpd.GeoDataFrame(columns=[
                'edge_id', 'way_id', 'highway', 'name', 'ref', 'oneway',
                'start_s', 'end_s', 'segment_length', 'n_points', 'density'
            ], crs=self.target_crs)
        
        elapsed = time.time() - start_time
        self.logger.info(f"✅ Coverage segments created in {elapsed:.1f}s")
        self.logger.info(f"📊 {len(segments_gdf):,} segments created")
        
        if len(segments_gdf) > 0:
            total_length = segments_gdf['segment_length'].sum() / 1000  # km
            self.logger.info(f"📏 Total coverage length: {total_length:.1f} km")
        
        return segments_gdf
    
    def dissolve_adjacent_segments(self, segments_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Dissolve adjacent segments with same attributes"""
        if len(segments_gdf) == 0:
            return segments_gdf
        
        self.logger.info("🔗 Dissolving adjacent segments...")
        start_time = time.time()
        
        # Group by attributes that should match
        group_cols = ['way_id', 'highway', 'name', 'ref', 'oneway']
        # Only use columns that exist
        group_cols = [col for col in group_cols if col in segments_gdf.columns]
        
        self.logger.info(f"🔍 Available columns: {list(segments_gdf.columns)}")
        self.logger.info(f"🔍 Group columns: {group_cols}")
        
        if not group_cols:
            self.logger.info("⚠️  No valid grouping columns found, returning original segments")
            return segments_gdf
        
        # Replace None values with empty strings for grouping
        segments_copy = segments_gdf.copy()
        for col in group_cols:
            if col in segments_copy.columns:
                segments_copy[col] = segments_copy[col].fillna('')
        
        dissolved_segments = []
        total_groups = segments_copy.groupby(group_cols).ngroups
        self.logger.info(f"🔍 Processing {total_groups} groups")
        
        for group_idx, (group_vals, group_df) in enumerate(segments_copy.groupby(group_cols)):
            # Sort by start_s
            group_df = group_df.sort_values('start_s').reset_index(drop=True)
            
            # Find contiguous segments
            contiguous_groups = []
            current_group = [group_df.iloc[0]]
            
            for i in range(1, len(group_df)):
                prev_segment = current_group[-1]
                curr_segment = group_df.iloc[i]
                
                # Check if segments are adjacent (within 1m gap)
                gap = curr_segment.start_s - prev_segment.end_s
                if gap <= 1.0:
                    current_group.append(curr_segment)
                else:
                    contiguous_groups.append(current_group)
                    current_group = [curr_segment]
            
            contiguous_groups.append(current_group)
            
            # Dissolve each contiguous group
            for group in contiguous_groups:
                if len(group) == 1:
                    dissolved_segments.append(group[0])
                else:
                    # Merge geometries and attributes
                    geometries = [seg.geometry for seg in group]
                    merged_geom = unary_union(geometries)
                    
                    dissolved_segment = group[0].copy()
                    dissolved_segment.geometry = merged_geom
                    dissolved_segment.start_s = group[0].start_s
                    dissolved_segment.end_s = group[-1].end_s
                    dissolved_segment.segment_length = sum(seg.segment_length for seg in group)
                    dissolved_segment.n_points = sum(seg.n_points for seg in group)
                    dissolved_segment.density = dissolved_segment.n_points / dissolved_segment.segment_length
                    
                    dissolved_segments.append(dissolved_segment)
        
        self.logger.info(f"🔍 Created {len(dissolved_segments)} dissolved segments")
        
        # Convert list of series to DataFrame first
        if dissolved_segments:
            dissolved_df = pd.DataFrame(dissolved_segments)
            dissolved_gdf = gpd.GeoDataFrame(dissolved_df, geometry='geometry', crs=self.target_crs)
        else:
            # Create empty GeoDataFrame with proper geometry column
            dissolved_gdf = gpd.GeoDataFrame([], geometry=gpd.GeoSeries([], name='geometry'), crs=self.target_crs)
        
        elapsed = time.time() - start_time
        reduction = len(segments_gdf) - len(dissolved_gdf)
        self.logger.info(f"✅ Dissolving completed in {elapsed:.1f}s")
        self.logger.info(f"📊 {len(dissolved_gdf):,} segments (reduced by {reduction:,})")
        
        return dissolved_gdf
    
    def export_results(self, assignments_df: pd.DataFrame, coverage_gdf: gpd.GeoDataFrame, 
                      output_dir: Union[str, Path]):
        """Export results to files"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"💾 Exporting results to {output_path}")
        
        # Export assignments
        assignments_file = output_path / "panos_snapped.parquet"
        assignments_df.to_parquet(assignments_file, index=False)
        self.logger.info(f"✅ Assignments saved: {assignments_file}")
        
        # Convert coverage to WGS84 for export
        if len(coverage_gdf) > 0:
            coverage_wgs84 = coverage_gdf.to_crs(self.source_crs)
            
            # Export coverage as GPKG
            coverage_gpkg = output_path / "coverage_lines.gpkg"
            coverage_wgs84.to_file(coverage_gpkg, driver='GPKG')
            self.logger.info(f"✅ Coverage GPKG saved: {coverage_gpkg}")
            
            # Export coverage as GeoJSON
            coverage_geojson = output_path / "coverage_lines.geojson"
            coverage_wgs84.to_file(coverage_geojson, driver='GeoJSON')
            self.logger.info(f"✅ Coverage GeoJSON saved: {coverage_geojson}")
        
        # Export summary statistics
        stats = {
            'generation_info': {
                'timestamp': datetime.now().isoformat(),
                'total_panoramas': len(self.panos_gdf),
                'snapped_panoramas': len(assignments_df),
                'coverage_segments': len(coverage_gdf),
                'snap_success_rate': len(assignments_df) / len(self.panos_gdf) * 100
            },
            'quality_distribution': assignments_df['match_quality'].value_counts().to_dict(),
            'coverage_statistics': {
                'total_segments': len(coverage_gdf),
                'total_length_km': coverage_gdf['segment_length'].sum() / 1000 if len(coverage_gdf) > 0 else 0,
                'avg_segment_length_m': coverage_gdf['segment_length'].mean() if len(coverage_gdf) > 0 else 0,
                'avg_point_density': coverage_gdf['density'].mean() if len(coverage_gdf) > 0 else 0
            },
            'parameters': {
                'search_radius_m': self.config.r_query,
                'max_snap_distance_m': self.config.d_max,
                'max_heading_deviation_deg': self.config.theta_max,
                'coverage_radius_m': self.config.coverage_radius
            }
        }
        
        stats_file = output_path / "coverage_statistics.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        self.logger.info(f"✅ Statistics saved: {stats_file}")
    
    def process(self, osm_file: Union[str, Path], panoramas_file: Union[str, Path], 
                output_dir: Union[str, Path]):
        """
        Main processing pipeline
        
        Args:
            osm_file: Path to OSM network file
            panoramas_file: Path to panoramas GeoJSON
            output_dir: Output directory for results
        """
        try:
            total_start = time.time()
            
            # Setup CRS transformers
            self._setup_crs_transformers()
            
            # 1. Load data
            self.edges_gdf = self.load_osm_network(osm_file)
            self.panos_gdf = self.load_panoramas(panoramas_file)
            
            # 2. Build spatial index
            self.build_spatial_index()
            
            # 3. Snap panoramas
            self.snap_all_panoramas()
            
            if not self.assignments:
                self.logger.error("❌ No successful snaps - cannot proceed")
                return
            
            # 4. Calculate coverage intervals
            self.calculate_coverage_intervals()
            
            # 5. Create coverage segments
            coverage_gdf = self.create_coverage_segments()
            
            # 6. Dissolve adjacent segments
            if len(coverage_gdf) > 0:
                coverage_gdf = self.dissolve_adjacent_segments(coverage_gdf)
            
            # 7. Export results
            assignments_df = pd.DataFrame(self.assignments)
            self.export_results(assignments_df, coverage_gdf, output_dir)
            
            # Final summary
            total_elapsed = time.time() - total_start
            self.logger.info(f"\n🎉 Processing completed in {total_elapsed:.1f}s!")
            self.logger.info(f"📊 Summary:")
            self.logger.info(f"   • {len(self.panos_gdf):,} input panoramas")
            self.logger.info(f"   • {len(assignments_df):,} successfully snapped")
            self.logger.info(f"   • {len(coverage_gdf):,} coverage segments generated")
            if len(coverage_gdf) > 0:
                total_km = coverage_gdf['segment_length'].sum() / 1000
                self.logger.info(f"   • {total_km:.1f} km total coverage")
            
        except Exception as e:
            self.logger.error(f"❌ Processing failed: {e}")
            raise


def main():
    """Main entry point"""
    print("🇮🇸 JA360 Coverage Generator")
    print("="*50)
    
    # Configuration
    base_dir = Path("../../data/ja360")
    processed_dir = base_dir / "processed"
    output_dir = base_dir / "coverage"
    
    # Input files
    panoramas_file = processed_dir / "ja360_iceland_panoramas.geojson"
    
    # OSM file (user needs to provide this)
    # Download from: https://download.geofabrik.de/europe/iceland-latest.osm.pbf
    # Convert to GPKG: ogr2ogr -f GPKG iceland_roads.gpkg iceland-latest.osm.pbf lines
    osm_file = base_dir / "iceland_roads.gpkg"
    
    # Check input files
    if not panoramas_file.exists():
        print(f"❌ Panoramas file not found: {panoramas_file}")
        print(f"💡 Run ja_processor.py first to generate the panoramas file")
        sys.exit(1)
    
    if not osm_file.exists():
        print(f"❌ OSM file not found: {osm_file}")
        print(f"💡 Download Iceland OSM data and convert to GPKG:")
        print(f"   1. Download: https://download.geofabrik.de/europe/iceland-latest.osm.pbf")
        print(f"   2. Convert: ogr2ogr -f GPKG {osm_file} iceland-latest.osm.pbf lines")
        print(f"   3. Or use QGIS to export highway features to GPKG")
        sys.exit(1)
    
    try:
        # Create configuration
        config = SnapConfig()
        
        # Initialize generator
        generator = JA360CoverageGenerator(config)
        
        # Run processing
        generator.process(osm_file, panoramas_file, output_dir)
        
    except KeyboardInterrupt:
        print("\n⚠️  Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()