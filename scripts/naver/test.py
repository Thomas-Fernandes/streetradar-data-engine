#!/usr/bin/env python3
"""
Simple PMTiles Viewer - Flask App

A lightweight web application to visualize PMTiles files.
Uses MapLibre GL JS with PMTiles plugin for optimal performance.

Features:
- Serves PMTiles files with proper headers
- Interactive map centered on South Korea
- Simple and reliable PMTiles rendering

Usage:
    python pmtiles_viewer.py
    
    Then open: http://localhost:5000

Author: Data Collection Team
Date: 2024
"""

import os
import sys
from pathlib import Path
from flask import Flask, render_template_string, send_from_directory, jsonify
import mimetypes

# Add PMTiles MIME type
mimetypes.add_type('application/x-protobuf', '.pmtiles')

app = Flask(__name__)

# Configuration
DATA_DIR = Path("../../data/naver/usage/tiles")
PMTILES_FILE = "naver.pmtiles"

# HTML Template with MapLibre GL JS and PMTiles
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PMTiles Viewer - Naver Korea</title>
    
    <!-- MapLibre GL CSS -->
    <link href="https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.css" rel="stylesheet" />
    
    <style>
        body, html {
            margin: 0;
            padding: 0;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            height: 100%;
        }
        
        #map {
            height: 100vh;
            width: 100%;
        }
        
        .info-panel {
            position: absolute;
            top: 10px;
            right: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            z-index: 1000;
            min-width: 250px;
            max-width: 300px;
        }
        
        .info-panel h3 {
            margin: 0 0 10px 0;
            color: #333;
            font-size: 16px;
        }
        
        .info-panel p {
            margin: 5px 0;
            font-size: 14px;
            color: #666;
        }
        
        .status {
            padding: 8px 12px;
            border-radius: 4px;
            font-weight: bold;
            margin: 10px 0;
            font-size: 13px;
        }
        
        .status.loading {
            background: #fff3cd;
            color: #856404;
        }
        
        .status.success {
            background: #d4edda;
            color: #155724;
        }
        
        .status.error {
            background: #f8d7da;
            color: #721c24;
        }
        
        .controls {
            background: rgba(255, 255, 255, 0.9);
            padding: 10px;
            border-radius: 4px;
            margin-top: 10px;
        }
        
        .controls label {
            display: block;
            margin: 8px 0;
            font-size: 12px;
            color: #555;
        }
        
        .controls input[type="range"] {
            width: 100%;
            margin: 5px 0;
        }
        
        .stats {
            font-size: 11px;
            color: #888;
            margin-top: 10px;
            padding-top: 10px;
            border-top: 1px solid #eee;
        }
        
        .stats p {
            margin: 3px 0;
        }
    </style>
</head>
<body>
    <div id="map"></div>
    
    <div class="info-panel">
        <h3>🗺️ PMTiles Viewer</h3>
        <p><strong>Dataset:</strong> Naver Korea</p>
        <p><strong>File:</strong> {{ PMTILES_FILE }}</p>
        
        <div id="status" class="status loading">
            Initializing map...
        </div>
        
        <div class="controls">
            <label>
                Layer Opacity
                <input type="range" id="opacity" min="0" max="100" value="80" />
                <span id="opacity-value">80%</span>
            </label>
        </div>
        
        <div class="stats" id="stats">
            <p><strong>Coordinates:</strong> <span id="coordinates">-</span></p>
            <p><strong>Zoom:</strong> <span id="zoom-level">-</span></p>
            <p><strong>Bounds:</strong> <span id="bounds">-</span></p>
        </div>
    </div>

    <!-- MapLibre GL JS -->
    <script src="https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.js"></script>
    
    <!-- PMTiles JS -->
    <script src="https://unpkg.com/pmtiles@2.11.0/dist/index.js"></script>
    
    <script>
        // Initialize map
        const map = new maplibregl.Map({
            container: 'map',
            style: {
                version: 8,
                sources: {
                    'osm': {
                        type: 'raster',
                        tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
                        tileSize: 256,
                        attribution: '© OpenStreetMap contributors'
                    }
                },
                layers: [
                    {
                        id: 'osm',
                        type: 'raster',
                        source: 'osm'
                    }
                ]
            },
            center: [127.5, 36.5], // South Korea center
            zoom: 6
        });

        // UI elements
        const statusDiv = document.getElementById('status');
        const coordinatesSpan = document.getElementById('coordinates');
        const zoomLevelSpan = document.getElementById('zoom-level');
        const boundsSpan = document.getElementById('bounds');
        const opacitySlider = document.getElementById('opacity');
        const opacityValue = document.getElementById('opacity-value');

        // Update status
        function updateStatus(message, type) {
            statusDiv.textContent = message;
            statusDiv.className = `status ${type}`;
        }

        // Update stats
        function updateStats() {
            const center = map.getCenter();
            const zoom = map.getZoom();
            const bounds = map.getBounds();
            
            coordinatesSpan.textContent = `${center.lat.toFixed(4)}, ${center.lng.toFixed(4)}`;
            zoomLevelSpan.textContent = zoom.toFixed(1);
            boundsSpan.textContent = `${bounds.getNorth().toFixed(3)}, ${bounds.getWest().toFixed(3)}`;
        }

        // Map event handlers
        map.on('load', async function() {
            updateStatus('Loading PMTiles...', 'loading');
            
            try {
                // Add PMTiles protocol
                let protocol = new pmtiles.Protocol();
                maplibregl.addProtocol("pmtiles", protocol.tile);
                
                // Add PMTiles source and layer
                map.addSource('pmtiles-source', {
                    type: 'vector',
                    url: `pmtiles:///tiles/{{ PMTILES_FILE }}`
                });
                
                // Add different layers for different geometry types
                // You might need to adjust layer names based on your PMTiles content
                
                // Add points layer
                map.addLayer({
                    id: 'points',
                    type: 'circle',
                    source: 'pmtiles-source',
                    'source-layer': 'naver', // Adjust this based on your layer name
                    filter: ['==', '$type', 'Point'],
                    paint: {
                        'circle-radius': 3,
                        'circle-color': '#ff7800',
                        'circle-opacity': 0.8
                    }
                });
                
                // Add lines layer
                map.addLayer({
                    id: 'lines',
                    type: 'line',
                    source: 'pmtiles-source',
                    'source-layer': 'naver',
                    filter: ['==', '$type', 'LineString'],
                    paint: {
                        'line-color': '#3388ff',
                        'line-width': 2,
                        'line-opacity': 0.8
                    }
                });
                
                // Add fills layer
                map.addLayer({
                    id: 'fills',
                    type: 'fill',
                    source: 'pmtiles-source',
                    'source-layer': 'naver',
                    filter: ['==', '$type', 'Polygon'],
                    paint: {
                        'fill-color': '#90EE90',
                        'fill-opacity': 0.4,
                        'fill-outline-color': '#006400'
                    }
                });
                
                updateStatus('✅ PMTiles loaded successfully!', 'success');
                
                // Add click handler for popups
                map.on('click', ['points', 'lines', 'fills'], function(e) {
                    if (e.features.length > 0) {
                        const feature = e.features[0];
                        
                        let html = '<div style="max-width: 200px;">';
                        html += `<strong>Type:</strong> ${feature.geometry.type}<br>`;
                        
                        if (feature.properties) {
                            Object.keys(feature.properties).forEach(key => {
                                html += `<strong>${key}:</strong> ${feature.properties[key]}<br>`;
                            });
                        }
                        html += '</div>';
                        
                        new maplibregl.Popup()
                            .setLngLat(e.lngLat)
                            .setHTML(html)
                            .addTo(map);
                    }
                });
                
                // Change cursor on hover
                map.on('mouseenter', ['points', 'lines', 'fills'], function() {
                    map.getCanvas().style.cursor = 'pointer';
                });
                
                map.on('mouseleave', ['points', 'lines', 'fills'], function() {
                    map.getCanvas().style.cursor = '';
                });
                
            } catch (error) {
                console.error('Error loading PMTiles:', error);
                updateStatus(`❌ Error: ${error.message}`, 'error');
            }
        });

        // Opacity control
        opacitySlider.addEventListener('input', function() {
            const opacity = this.value / 100;
            opacityValue.textContent = this.value + '%';
            
            // Update opacity for all layers
            if (map.getLayer('points')) {
                map.setPaintProperty('points', 'circle-opacity', opacity);
            }
            if (map.getLayer('lines')) {
                map.setPaintProperty('lines', 'line-opacity', opacity);
            }
            if (map.getLayer('fills')) {
                map.setPaintProperty('fills', 'fill-opacity', opacity * 0.5); // Half opacity for fills
            }
        });

        // Update stats on map move
        map.on('move', updateStats);
        
        // Add navigation controls
        map.addControl(new maplibregl.NavigationControl(), 'top-left');
        
        // Add scale control
        map.addControl(new maplibregl.ScaleControl(), 'bottom-left');
        
        console.log('🗺️ PMTiles Viewer initialized with MapLibre GL JS');
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Main page with the map viewer."""
    # Check if PMTiles file exists
    pmtiles_path = DATA_DIR / PMTILES_FILE
    
    if not pmtiles_path.exists():
        error_html = f"""
        <html>
        <head><title>PMTiles File Not Found</title></head>
        <body style="font-family: Arial; padding: 50px; text-align: center;">
            <h1>❌ PMTiles File Not Found</h1>
            <p>Expected file: <code>{pmtiles_path}</code></p>
            <p>Please make sure the PMTiles file has been generated first.</p>
            <p>Run: <code>python naver_pmtiles.py</code></p>
            <br>
            <p><a href="/info">Check file info</a></p>
        </body>
        </html>
        """
        return error_html, 404
    
    return render_template_string(HTML_TEMPLATE, PMTILES_FILE=PMTILES_FILE)

@app.route('/tiles/<filename>')
def serve_tiles(filename):
    """Serve PMTiles files with proper headers."""
    if not filename.endswith('.pmtiles'):
        return "Invalid file type", 400
    
    if filename != PMTILES_FILE:
        return "File not found", 404
    
    # Serve with proper headers for PMTiles
    response = send_from_directory(
        DATA_DIR, 
        filename,
        mimetype='application/x-protobuf'
    )
    
    # Add CORS headers for PMTiles
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, HEAD, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Range'
    response.headers['Access-Control-Expose-Headers'] = 'Accept-Ranges, Content-Encoding, Content-Length, Content-Range'
    response.headers['Accept-Ranges'] = 'bytes'
    
    return response

@app.route('/info')
def info():
    """Get information about the PMTiles file."""
    pmtiles_path = DATA_DIR / PMTILES_FILE
    
    if not pmtiles_path.exists():
        return jsonify({"error": "PMTiles file not found", "path": str(pmtiles_path)}), 404
    
    file_size = pmtiles_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    
    return jsonify({
        "filename": PMTILES_FILE,
        "path": str(pmtiles_path),
        "size_bytes": file_size,
        "size_mb": round(file_size_mb, 2),
        "exists": True,
        "url": f"http://localhost:5000/tiles/{PMTILES_FILE}"
    })

@app.errorhandler(404)
def not_found(error):
    """Custom 404 page."""
    return """
    <html>
    <head><title>404 - Not Found</title></head>
    <body style="font-family: Arial; padding: 50px; text-align: center;">
        <h1>404 - Page Not Found</h1>
        <p><a href="/">← Back to PMTiles Viewer</a></p>
        <p><a href="/info">File Info</a></p>
    </body>
    </html>
    """, 404

def main():
    """Run the Flask development server."""
    print("🚀 PMTiles Viewer Starting...")
    print("=" * 50)
    
    # Check if PMTiles file exists
    pmtiles_path = DATA_DIR / PMTILES_FILE
    
    if pmtiles_path.exists():
        file_size_mb = pmtiles_path.stat().st_size / (1024 * 1024)
        print(f"✅ PMTiles file found: {pmtiles_path}")
        print(f"📦 File size: {file_size_mb:.1f} MB")
    else:
        print(f"⚠️  PMTiles file not found: {pmtiles_path}")
        print(f"💡 Generate it first with: python naver_pmtiles.py")
    
    print(f"📁 Serving from: {DATA_DIR}")
    print(f"🌐 Open in browser: http://localhost:5000")
    print(f"📊 Info endpoint: http://localhost:5000/info")
    print("=" * 50)
    
    # Run Flask app
    app.run(
        host='0.0.0.0',  # Allow external connections
        port=5000,
        debug=True,
        use_reloader=False  # Prevent double startup in debug mode
    )

if __name__ == "__main__":
    main()