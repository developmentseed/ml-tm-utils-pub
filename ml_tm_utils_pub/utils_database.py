"""
Utility functions for storing ML-derived task annotations related to buildings.

The functions here are meant to help organize data in databases stored outside
of HOT's Tasking Manager.
"""


from sqlalchemy import (Column, Integer, String, Float,
                        ForeignKey)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from ml_tm_utils_pub.utils_geodata import (get_tile_pyramid)

#######################################
# Set the declarative base to prep creation of SQL classes
Base = declarative_base()


class Project(Base):
    """Project class meant to hold information on mapping projects in TM.

    Attributes
    ----------
    id: int
        The object's UID for the relational DB
    tm_index: int
        ID of the project on Tasking Manager's servers
    md5_hash: str
        MD5 hash of the project geometry. Useful for checking if a split
        occured
    json_geometry: str
        Stripped down version of the geojson project geometry.
    """

    __tablename__ = 'ml_projects'
    id = Column(Integer, primary_key=True)
    tm_index = Column(Integer)
    md5_hash = Column(String)
    json_geometry = Column(String)

    # Add a relationship with the tile prediction class
    building_tiles = relationship(
        "TilePredBA", back_populates="project")

    def __repr__(self):
        """Define string representation."""
        return "<Project(TM index={}, md5_hash={}, {} tiles>".format(
            self.tm_index, self.md5_hash, len(self.building_tiles))


class TilePredBA(Base):
    """Tile prediction building area (storing both ML estimate and OSM)

    Attributes
    ----------
    id: int
        The tile objects UID for the relational DB
    project_id: int
        Project ID keyed to the project table
    tile_index: str
        Tile index in string format specifying the x/y/z tile coords.
    building_area_ml: float
        Total building area for a tile as predicted by the ML algorithm
    building_area_osm: float
        Total building area for a tile mapped in OSM
    """

    __tablename__ = 'tile_pred_buildings'
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('ml_projects.id'))
    tile_index = Column(String)
    building_area_ml = Column(Float)
    building_area_osm = Column(Float)

    # Add a relationship with the project class
    project = relationship('Project', back_populates='building_tiles')

    def __repr__(self):
        """Define string representation."""
        return ("<TilePredBA(Project={}, Tile Index={} "
                "Building Area ML={}, Building Area OSM={}>").format(
                    self.project.tm_index, self.tile_index,
                    self.building_area_ml, self.building_area_osm)


def get_total_tiles_building_area(tile_ind_list, session):
    """Get total area of all tile indices specified in a list.

    Parameters
    -----------
    tile_ind_list: list of str
        List of tile indices to query
    session: sqlalchemy.orm.session.Session
        Handle to database

    Returns
    -------
    total_area_ml: float
        Sum of predicted building area for all tiles
    total_area_osm: float
        Sum of mapped building area in OSM for all tiles
    """

    total_area_ml, total_area_osm = 0, 0
    for row in session.query(TilePredBA).filter(
            TilePredBA.tile_index.in_(tile_ind_list)):
        total_area_ml += row.building_area_ml
        total_area_osm += row.building_area_osm

    return total_area_ml, total_area_osm


def augment_geojson_building_area(project, session):
    """Add building area information to each tile in a geojson dict.

    Parameters
    ----------
    project: dict
        geojson to be augmented with new information
    session: sqlalchemy.orm.session.Session
        Handle to database
    """

    # Loop through tasks in TM visualization
    for ti, task in enumerate(project['tasks']['features']):

        # Get total area
        tile_dict = dict(x=task['properties']['taskX'],
                         y=task['properties']['taskY'],
                         z=task['properties']['taskZoom'])
        child_tiles = get_tile_pyramid(tile_dict, max_zoom=18)

        area_ml, area_osm = get_total_tiles_building_area(child_tiles, session)

        # Add information to geojson
        task['properties']['building_area_ml_pred'] = area_ml
        task['properties']['building_area_osm'] = area_osm
        project['tasks']['features'][ti] = task

    # Return geojson
    return project


def update_db_project(proj_id, geojson, geojson_hash, session):
    """Update a project geojson and hash

    Parameters
    ----------
    proj_id: int
        TM Project ID corresponding to database entry for updating
    geojson: str
        Geojson string of project geometry
    geojson_hash: str
        MD5 hash of geojson object
    session: sqlalchemy.orm.session.Session
        Handle to database
    """

    project = session.query(Project).filter(
        Project.tm_index == proj_id).one()
    project.json_geometry = geojson
    project.md5_hash = geojson_hash
