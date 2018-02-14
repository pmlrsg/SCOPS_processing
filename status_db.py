import sqlite3
import os
from scops import scops_common

def create_db():
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute("CREATE TABLE flightlines (id INTEGER PRIMARY KEY AUTOINCREMENT, processing_id STRING, name STRING, stage STRING, progress FLOAT, filesize FLOAT, bytesize STRING, flag STRING, link STRING, zipsize FLOAT, zipbyte STRING);")
    conn.commit()
    c.close()

def insert_line_into_db(processing_id, name, stage, progress, filesize, bytesize, flag, link, zipsize, zipbyte):
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
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute("SELECT * FROM flightlines WHERE processing_id IS ?", [processing_id])
    lines = c.fetchall()
    conn.commit()
    c.close()
    return lines

def get_line_status_from_db(processing_id, line_name):
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute("SELECT stage FROM flightlines WHERE processing_id IS ? AND name IS ?", [processing_id, line_name])
    line = c.fetchone()
    conn.commit()
    c.close()
    return line

def update_status(processing_id, line, status):
    conn = sqlite3.connect(scops_common.DB_LOCATION)
    c = conn.cursor()
    c.execute("UPDATE flightlines SET stage = ? WHERE processing_id IS ? AND name IS ?", [status, processing_id, line])
    if "ERROR" in status:
        c.execute("UPDATE flightlines SET flag = ? WHERE processing_id IS ? AND name IS ?", [1, processing_id, line])
    conn.commit()
    c.close()

def update_progress_details(processing_id, line, progress, filesize, bytesize, zipsize, zipbyte):
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
    print "making db"
    create_db()
