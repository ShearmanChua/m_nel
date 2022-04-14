from datetime import datetime 

def contains_techscan_date(string):
    """
    Usage:
    -----
    >>> contains_techscan_date( "Techscan2021091201" )
    True
    """
    if 'techscan' in string.lower():
        try:
            string.lower().replace('techscan','')
            return True
        except:
            pass
    return False

def parse_date_from_techscan_folder(string):
    """
    Usage:
    -----
    >>> parse_date_from_techscan_folder( "Techscan2021091201" )
    2021-09-12 00:00:00
    """
    datestring = string.lower().replace('techscan','')
    year = int(datestring[:4])
    month = int(datestring[4:6])
    day = int(datestring[6:8])
    return datetime(year=year, month=month, day=day)

def parse_date_from_filename(filename):
    """
    usage:
    ------
    >>> parse_date_from_filename( 'data/Scanned/20210912/Techscan2021091201.zip/AI_IRC/00291174/AI_Weekly__U_S__agencies_are_increasing_their_AI_investments.HTML')
    2021-09-12 00:00:00
    """
    folders = filename.split('/')
    date = [parse_date_from_techscan_folder(folder) for folder in folders if contains_techscan_date(folder)]
    if date: return date[0]
    return datetime.now()