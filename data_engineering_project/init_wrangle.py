# The main purpose of this file is just to create the schema of the database

from data_engineering_project.db_connection import get_connection


def replace_database():
    """This function drops the current database and creates a new one. You have to confirm this action in the terminal if the database file already exists."""
    conn = get_connection()
    does_table_exist = conn.query("SELECT table_name FROM information_schema.tables WHERE table_name = 'Measurement';")
    does_table_exist_shape = does_table_exist.shape

    table_exists = does_table_exist_shape[0]
    table_exists = bool(table_exists)

    if table_exists:
        print("Measurement table exists")
        n_records_q = conn.execute("select count(*) from Measurement").fetchone()
        n_records = n_records_q[0]
        while not len(inp := input(f"Are you sure you want to recreate the table? This will drop {n_records} records (y/n):")):
            pass
        if inp not in ("y", "Y"):
            print("Not creating table and quiting")
            exit()
        else:
            conn.execute("DROP TABLE IF EXISTS Measurement;")
    create_database_if_not_exists()


def create_database_if_not_exists():
    """ This function creates the database if it does not exist yet"""
    conn = get_connection()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS Measurement (
        station    TEXT NOT NULL,
        time BIGINT NOT NULL CHECK ( time > 0 ),
        wsi        TEXT,
        stationname  TEXT,
        lat        DOUBLE ,
        lon        DOUBLE,
        height     DOUBLE,
        D1H        DOUBLE,
        dd         DOUBLE,
        dn         DOUBLE,
        dr         DOUBLE,
        dsd        DOUBLE,
        dx         DOUBLE,
        ff         DOUBLE,
        ffs        DOUBLE,
        fsd        DOUBLE,
        fx         DOUBLE,
        fxs        DOUBLE,
        gff        DOUBLE,
        gffs       DOUBLE,
        h          DOUBLE,
        h1         DOUBLE,
        h2         DOUBLE,
        h3         DOUBLE,
        hc         DOUBLE,
        hc1        DOUBLE,
        hc2        DOUBLE,
        hc3        DOUBLE,
        n          DOUBLE,
        n1         DOUBLE,
        n2         DOUBLE,
        n3         DOUBLE,
        nc         DOUBLE,
        nc1        DOUBLE,
        nc2        DOUBLE,
        nc3        DOUBLE,
        p0         DOUBLE,
        pp         DOUBLE,
        pg         DOUBLE,
        pr         DOUBLE,
        ps         DOUBLE,
        pwc        DOUBLE,
        Q1H        DOUBLE,
        Q24H       DOUBLE,
        qg         DOUBLE,
        qgn        DOUBLE,
        qgx        DOUBLE,
        qnh        DOUBLE,
        R12H       DOUBLE,
        R1H        DOUBLE,
        R24H       DOUBLE,
        R6H        DOUBLE,
        rg         DOUBLE,
        rh         DOUBLE,
        rh10       DOUBLE,
        Sav1H      DOUBLE,
        Sax1H      DOUBLE,
        Sax3H      DOUBLE,
        Sax6H      DOUBLE,
        sq         DOUBLE,
        ss         DOUBLE,
        Sx1H       DOUBLE,
        Sx3H       DOUBLE,
        Sx6H      DOUBLE,
        t10        DOUBLE,
        ta         DOUBLE,
        tb         DOUBLE,
        tb1        DOUBLE,
        Tb1n6      DOUBLE,
        Tb1x6      DOUBLE,
        tb2        DOUBLE,
        Tb2n6      DOUBLE,
        Tb2x6      DOUBLE,
        tb3        DOUBLE,
        tb4        DOUBLE,
        tb5        DOUBLE,
        td         DOUBLE,
        td10       DOUBLE,
        tg         DOUBLE,
        tgn        DOUBLE,
        Tgn12      DOUBLE,
        Tgn14      DOUBLE,
        Tgn6       DOUBLE,
        tn         DOUBLE,
        Tn12       DOUBLE,
        Tn14       DOUBLE,
        Tn6        DOUBLE,
        tsd        DOUBLE,
        tx         DOUBLE,
        Tx12       DOUBLE,
        Tx24       DOUBLE,
        Tx6        DOUBLE,
        vv         DOUBLE,
        W10        DOUBLE,
        "W10-10"   DOUBLE,
        ww         DOUBLE,
        "ww-10"    DOUBLE,
        zm         DOUBLE,
        PRIMARY KEY (time, station)
        );
    """)
    print("Created Measurement table!")


