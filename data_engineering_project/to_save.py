from typing import Literal, Set

## Update both, idk how to make a type out of the type so for now just have 2

ALL_KNMI_VARIABLES = frozenset((
    "station", "time", "wsi", "stationname", "lat", "lon", "height", "D1H", "dd", "dn", "dr", "dsd", "dx", "ff", "ffs",
    "fsd", "fx", "fxs", "gff", "gffs", "h", "h1", "h2", "h3", "hc", "hc1", "hc2", "hc3", "n", "n1", "n2", "n3",
    "nc", "nc1", "nc2", "nc3", "p0", "pp", "pg", "pr", "ps", "pwc", "Q1H", "Q24H", "qg", "qgn", "qgx", "qnh", "R12H",
    "R1H", "R24H", "R6H", "rg", "rh", "rh10", "Sav1H", "Sax1H", "Sax3H", "Sax6H", "sq", "ss", "Sx1H", "Sx3H", "Sx6H",
    "t10", "ta", "tb", "tb1", "Tb1n6", "Tb1x6", "tb2", "Tb2n6", "Tb2x6", "tb3", "tb4", "tb5", "td", "td10", "tg",
    "tgn", "Tgn12", "Tgn14", "Tgn6", "tn", "Tn12", "Tn14", "Tn6", "tsd", "tx", "Tx12", "Tx24", "Tx6", "vv", "W10",
    "W10-10", "ww", "ww-10", "zm"))


KNMI_VARIABLE = Literal[
    "station", "time", "wsi", "stationname", "lat", "lon", "height", "D1H", "dd", "dn", "dr", "dsd", "dx", "ff", "ffs",
    "fsd", "fx", "fxs", "gff", "gffs", "h", "h1", "h2", "h3", "hc", "hc1", "hc2", "hc3", "n", "n1", "n2", "n3",
    "nc", "nc1", "nc2", "nc3", "p0", "pp", "pg", "pr", "ps", "pwc", "Q1H", "Q24H", "qg", "qgn", "qgx", "qnh", "R12H",
    "R1H", "R24H", "R6H", "rg", "rh", "rh10", "Sav1H", "Sax1H", "Sax3H", "Sax6H", "sq", "ss", "Sx1H", "Sx3H", "Sx6H",
    "t10", "ta", "tb", "tb1", "Tb1n6", "Tb1x6", "tb2", "Tb2n6", "Tb2x6", "tb3", "tb4", "tb5", "td", "td10", "tg",
    "tgn", "Tgn12", "Tgn14", "Tgn6", "tn", "Tn12", "Tn14", "Tn6", "tsd", "tx", "Tx12", "Tx24", "Tx6", "vv", "W10",
    "W10-10", "ww", "ww-10", "zm"]

KNMI_NUMERIC_VARIABLE = Literal[
     "time", "lat", "lon", "height", "D1H", "dd", "dn", "dr", "dsd", "dx", "ff", "ffs",
    "fsd", "fx", "fxs", "gff", "gffs", "h", "h1", "h2", "h3", "hc", "hc1", "hc2", "hc3", "n", "n1", "n2", "n3",
    "nc", "nc1", "nc2", "nc3", "p0", "pp", "pg", "pr", "ps", "pwc", "Q1H", "Q24H", "qg", "qgn", "qgx", "qnh", "R12H",
    "R1H", "R24H", "R6H", "rg", "rh", "rh10", "Sav1H", "Sax1H", "Sax3H", "Sax6H", "sq", "ss", "Sx1H", "Sx3H", "Sx6H",
    "t10", "ta", "tb", "tb1", "Tb1n6", "Tb1x6", "tb2", "Tb2n6", "Tb2x6", "tb3", "tb4", "tb5", "td", "td10", "tg",
    "tgn", "Tgn12", "Tgn14", "Tgn6", "tn", "Tn12", "Tn14", "Tn6", "tsd", "tx", "Tx12", "Tx24", "Tx6", "vv", "W10",
    "W10-10", "ww", "ww-10", "zm"]

NON_NUMERIC_KNMI_VARIABLES = frozenset(("station", "wsi", "stationname"))
NUMERIC_KNMI_VARIABLES : Set[KNMI_NUMERIC_VARIABLE] = ALL_KNMI_VARIABLES - NON_NUMERIC_KNMI_VARIABLES

ALL_KNMI_STATIONS = frozenset(("06237", "06258", "06310", "06316", "06331", "06350", "06377", "06251", "06267", "06273", "06279", "06280", "06283", "06313", "06340", "06343", "06235", "06239", "06242", "06285", "06290", "06308", "06315", "06375", "06391", "78873", "78990", "06201", "06204", "06208", "06209", "06211", "06215", "06229", "06236", "06240", "06257", "06260", "06270", "06330", "06348", "06205", "06214", "06225", "06248", "06252", "06317", "06321", "06344", "78871", "06216", "06238", "06269", "06277", "06312", "06324", "06356", "06370", "06203", "06233", "06249", "06275", "06319", "06320", "06323", "06380", "06207", "06278", "06286"))
KNMI_STATION = Literal["06237", "06258", "06310", "06316", "06331", "06350", "06377", "06251", "06267", "06273", "06279", "06280", "06283", "06313", "06340", "06343", "06235", "06239", "06242", "06285", "06290", "06308", "06315", "06375", "06391", "78873", "78990", "06201", "06204", "06208", "06209", "06211", "06215", "06229", "06236", "06240", "06257", "06260", "06270", "06330", "06348", "06205", "06214", "06225", "06248", "06252", "06317", "06321", "06344", "78871", "06216", "06238", "06269", "06277", "06312", "06324", "06356", "06370", "06203", "06233", "06249", "06275", "06319", "06320", "06323", "06380", "06207", "06278", "06286"]

