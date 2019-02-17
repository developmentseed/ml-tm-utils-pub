# Machine learning utilities for the Humanitarian OpenStreetMap Team's Tasking Manager

## Purpose

Development Seed is supporting HOT's goal of more tightly integrating ML into the Tasking Manger. The larger goal is to allow project creators to select ML models during project creation, run inference on the satellite imagery underlying a task,  and displaying the derived information to users.

The code base here provides utility functions to support part of this pipeline. Specifically, it provides database and geodata utility functions to receive and store ML predictions before routing this information to TM's servers via the Task Annotations API. This allows predictions to be stored at the finest granularity to keep raw prediction results and better respond to task splits.

## Utility functions

* Database utilities
  * store per-tile metrics derived from an ML model (e.g., building area in a single satellite image)
  * aggregate tile analytics (e.g., sum metrics for a set of tiles contained in one TM task)
  * store geojson geometry as a string and check for changes with a string hash (e.g., to monitor for task splits)

* GeoData utilities
  * Ingest a CSV containing key/value pairs as tile index/metric 
  * Strip a geojson to only its geometry
  * Hash a string and compare hash values (e.g., to check if two geometry strings are identical)
  * Augment a TM Project geojson dictionary with new task properties
  * Given a tile, get all children tiles down to an arbitrary zoom level
  * Make a windowed read into a cloud-optimized geotiff 
