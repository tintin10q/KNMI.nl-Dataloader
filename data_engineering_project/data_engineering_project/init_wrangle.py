

# The main purpose of this file is just to create the schema of the database


import duckdb


conn = duckdb.connect("central-hmni.duckdb", read_only=False)

conn.execute("""
CREATE TABLE t1 (
    time INTEGER NOT NULL CHECK ( time > 0 ),
    station TEXT NOT NULL,
    
    wsi        Float
    stationname Float etc , 
    lat        
    lon        
    height     
    D1H        
    dd         
    dn         
    dr         
    dsd        
    dx         
    ff         
    ffs        
    fsd        
    fx         
    fxs        
    gff        
    gffs       
    h          
    h1         
    h2         
    h3         
    hc         
    hc1        
    hc2        
    hc3        
    n          
    n1         
    n2         
    n3         
    nc         
    nc1        
    nc2        
    nc3        
    p0         
    pp         
    pg         
    pr         
    ps         
    pwc        
    Q1H        
    Q24H       
    qg         
    qgn        
    qgx        
    qnh        
    R12H       
    R1H        
    R24H       
    R6H        
    rg         
    rh         
    rh10       
    Sav1H      
    Sax1H      
    Sax3H      
    Sax6H      
    sq         
    ss         
    Sx1H       
    Sx3H       
   PSx6H        
   t10        
   ta         
    tb         
    tb1        
    Tb1n6      
    Tb1x6      
    tb2        
    Tb2n6      
    Tb2x6      
    tb3        
    tb4        
    tb5        
    td         
    td10       
    tg         
    tgn        
    Tgn12      
    Tgn14      
    Tgn6       
    tn         
    Tn12       
    Tn14       
    Tn6        
    tsd        
    tx         
    Tx12       
    Tx24       
    Tx6        
    vv         
    W10        
    W10-10     
    ww         
    ww-10      
    zm         
    RIMARY KEY (time, station), 
    );
""")