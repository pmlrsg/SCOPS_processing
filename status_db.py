import sqlite3
import os
from scops import scops_common

def create_db():
    """
    Creates the database with empty tables.
    """
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute("CREATE TABLE flightlines (id INTEGER PRIMARY KEY AUTOINCREMENT, processing_id STRING, name STRING, stage STRING, progress FLOAT, filesize FLOAT, bytesize STRING, flag STRING, link STRING, zipsize FLOAT, zipbyte STRING);")
    conn.commit()
    c.close()

def insert_line_into_db(processing_id, name, stage, progress, filesize, bytesize, flag, link, zipsize, zipbyte):
    """
    Inserts a line entry to the database.

    :param processing_id: string
    :param name: string
    :param stage: string
    :param progress: int
    :param filesize: int
    :param bytesize: string
    :param flag: bool
    :param link: string
    :param zipsize: int
    :param zipbyte: string
    """
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute('INSERT INTO flightlines VALUES (NULL,?,?,?,?,?,?,?,?,?,?)', [processing_id,
                                                                            name,
                                                                            stage,
                                                                            progress,
                                                                            filesize,
                                                                            bytesize,
                                                                            flag,
                                                                            link,
                                                                            zipsize,
                                                                            zipbyte])
    conn.commit()
    c.close()

def get_lines_from_db(processing_id):
    """
    Given a processing id returns all lines associated with the project.

    :param processing_id: string
    :return lines: list
    """
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute("SELECT * FROM flightlines WHERE processing_id IS ?", [processing_id])
    lines = c.fetchall()
    conn.commit()
    c.close()
    return lines

def get_line_status_from_db(processing_id, line_name):
    """
    Given a processing id and line name returns the line associated with the project.

    :param processing_id: string
    :param line_name: string
    :return lines: list
    """
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute("SELECT stage FROM flightlines WHERE processing_id IS ? AND name IS ?", [processing_id, line_name])
    line = c.fetchone()
    conn.commit()
    c.close()
    return line

def update_status(processing_id, line, status):
    """
    Given a processing id, line name and status updates the line status in the database.

    :param processing_id: string
    :param line_name: string
    :param status: string
    :return: None
    """
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute("UPDATE flightlines SET stage = ? WHERE processing_id IS ? AND name IS ?", [status, processing_id, line])
    if "ERROR" in status:
        c.execute("UPDATE flightlines SET flag = ? WHERE processing_id IS ? AND name IS ?", [1, processing_id, line])
    conn.commit()
    c.close()

def update_progress_details(processing_id, line, progress, filesize, bytesize, zipsize, zipbyte):
    """
    Updates the progress of a line in the database.

    :param processing_id: string
    :param line: string
    :param filesize: int
    :param bytesize: string
    :param zipsize: int
    :param zipbyte: string
    :return: None
    """
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute("UPDATE flightlines SET progress = ?, filesize = ?, bytesize = ?, zipsize = ?, zipbyte = ? WHERE processing_id = ? AND name = ?", [progress,
                                                                                                                                                             filesize,
                                                                                                                                                             bytesize,
                                                                                                                                                             zipsize,
                                                                                                                                                             zipbyte,
                                                                                                                                                             processing_id,
                                                                                                                                                             line])
    conn.commit()
    c.close()

if not os.path.isfile(scops_common.DB_LOCATION):
    #if it doesn't exist its time to make it, will run on first import.
    create_db()
