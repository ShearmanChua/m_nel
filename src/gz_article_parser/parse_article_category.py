def parse_tagging(filename):
    """
    usage:
    ------
    >>> parse_tagging_from_filename( 'data/Scanned/20210912/Techscan2021091201.zip/AI_IRC/00291174/AI_Weekly__U_S__agencies_are_increasing_their_AI_investments.HTML')
    AI_IRC
    """
    try:
        category = filename.split('.zip/')[1].split("/")[0]
        return category
    except:
        return "unknown"