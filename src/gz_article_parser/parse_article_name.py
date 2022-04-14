"""
Usage:
-----
>>> filename = 'data/Scanned/20210911/Techscan2021091101.zip/AI_IRC/00289998/Cyberjaya_s_new_masterplan_set_to_weave__interlock_its_disparate_parts___Part__.HTML'
>>> filepath_to_article_name(filename)
'Cyberjayab s new masterplan set to weave, interlock its disparate parts b  Part 2'
"""
get_last_path = lambda article_name: article_name.split('/')[-1]
remove_html_extension = lambda article_name: article_name.split('.')[0]
convert_filename_to_sentence = lambda article_name: ' '.join(article_name.replace('_',' ').split())
filepath_to_article_name = lambda article_name: convert_filename_to_sentence(remove_html_extension(get_last_path(article_name)))