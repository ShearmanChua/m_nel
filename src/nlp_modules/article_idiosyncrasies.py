from .abstract_classes import Pipe, Idiosyncrasy

class Idiosyncrasy_Cleaner(Pipe):
    def __init__(self):
        self.idiosyncrasies = [
            remove_advertisement_lines, 
            remove_cookies_articles,
            english_only,
            remove_dups
            ]

    def process(self, df):
        for idiosyncrasy in self.idiosyncrasies:
            df = idiosyncrasy().clean(df)
        return df

"""
Enter specific cleaning steps below
"""
class english_only(Idiosyncrasy):
    """
    Filter english articles
    """
    def clean(self, df, debug=False):
        if debug: print(f"\n  Clearing non-english articles ... \n  Number of non-english articles: {len(df.loc[df.language!='en'])}\n")
        return df.loc[df.language=='en']

class remove_dups(Idiosyncrasy):
    def clean(self, df):
        df.maintext = df.maintext.str.replace('Advertisement\n','')
        return df.drop_duplicates(subset='title').reset_index(drop=True)

class remove_advertisement_lines(Idiosyncrasy):
    """
    CNA has many advertisements. 
    News-please will generally parse them as single line "Advertisement"

    Example:
    {"authors": [], "date_download": "2021-07-19 13:43:35", "date_modify": null, "date_publish": "2021-05-31 19:20:03", "description": "MOSCOW: Russia said on Monday (May 31) it would send what it described as \"uncomfortable\" signals to the US ahead of a summit between the leaders ...", "filename": "https%3A%2F%2Fwww.channelnewsasia.com%2Fnews%2Fworld%2Funcomfortable-signals-putin-biden-summit-geneva-14920706.json", "image_url": "https://cna-sg-res.cloudinary.com/image/upload/q_auto,f_auto/image/14857546/16x9/991/557/d3faa3f1651b24ba03bfd6832d408687/cv/russia-putin-33634-jpg-1621595465.jpg", "language": "en", "localpath": null, "source_domain": "www.channelnewsasia.com", "maintext": "MOSCOW: Russia said on Monday (May 31) it would send what it described as \"uncomfortable\" signals to the US ahead of a summit between the leaders of the two countries next month and announced it was beefing up its western border militarily.\nThe comments came a day after US President Joe Biden said he would press Russian President Vladimir Putin to respect human rights when the two leaders meet in Geneva on Jun 16. Relations between the two powers are at post-Cold War lows.\nAdvertisement\nAdvertisement\n\"The Americans must assume that a number of signals from Moscow ... will be uncomfortable for them, including in the coming days,\" Sergei Ryabkov, Russia's deputy foreign minister, was quoted as saying by the RIA news agency.\nRyabkov said Russia would be prepared to respond to Biden's queries about human rights in Russia and said that Moscow was being more flexible than Washington when it came to drawing up an agenda for the summit, RIA reported.\nRussia's ties with the West are acutely strained over the jailing of Kremlin critic Alexei Navalny, a military build-up near Ukraine as well as allegations of election hacking.\nAdvertisement\nAdvertisement\nDefence Minister Sergei Shoigu said on Monday that the US and the NATO transatlantic alliance had recently increased military activity to the West of Russia, which required a response from Moscow.\n\"The actions of our Western colleagues are destroying the world's security system and force us to take adequate countermeasures,\" the Interfax news agency quoted Shoigu as saying.\n\"Around 20 military formations and units will be formed in the Western Military District by the end of the year,\" he was quoted as saying.", "title": "Russia tells US to expect 'uncomfortable' signals ahead of Putin-Biden summit", "title_page": null, "title_rss": null, "url": "https://www.channelnewsasia.com/news/world/uncomfortable-signals-putin-biden-summit-geneva-14920706", "misc": null}
    """
    def clean(self, df):
        df.maintext = df.maintext.str.replace('Advertisement\n','')
        return df

class remove_cookies_articles(Idiosyncrasy):
    """
    bbc.com has many articles that largely talk about cookies.
    sometimes these articles have descriptions, but the maintext is not meaningful

    Example:
    {"authors": ["Https", "Www.Facebook.Com Bbcnews"], "date_download": "2021-07-19 13:50:21", "date_modify": null, "date_publish": "2021-06-02 00:00:00", "description": "Hundreds of tonnes of fuel oil could leak into the sea with devastating impact on marine life.", "filename": "https%3A%2F%2Fwww.bbc.com%2Fnews%2Fworld-asia-57327300.amp.json", "image_url": "https://ichef.bbci.co.uk/news/1024/branded_news/8B4A/production/_118785653_067794244.jpg", "language": "en", "localpath": null, "source_domain": "www.bbc.com", "maintext": "We and our partners use technologies, such as cookies , and collect browsing data to give you the best online experience and to personalise the content and advertising shown to you. Please let us know if you agree.\nManage consent settings on AMP pages\nThese settings apply to AMP pages only. You may be asked to set these preferences again when you visit non-AMP BBC pages.\nThe lightweight mobile page you have visited has been built using Google AMP technology.\nStrictly necessary data collection\nTo make our web pages work, we store some limited information on your device without your consent.\nRead more about the essential information we store on your device to make our web pages work.\nWe use local storage to store your consent preferences on your device.\nOptional data collection\nWhen you consent to data collection on AMP pages you are consenting to allow us to display personalised ads that are relevant to you when you are outside of the UK.\nRead more about how we personalise ads in the BBC and our advertising partners.\nYou can choose not to receive personalised ads by clicking “Reject data collection and continue” below. Please note that you will still see advertising, but it will not be personalised to you.\nYou can change these settings by clicking “Ad Choices / Do not sell my info” in the footer at any time.", "title": "Fears of environmental disaster as oil-laden ship sinks off Sri Lanka", "title_page": null, "title_rss": null, "url": "https://www.bbc.com/news/world-asia-57327300.amp", "misc": null}
    """
    def clean(self, df, debug=False):
        maintext_describes_cookies_agreement_mask = (df.maintext.str.contains("We and our partners use technologies, such as cookies")) | (df.maintext.isnull())
        description_is_valid = (~df.description.isnull()) & (df.description.apply(str).apply(len)>0)

        if debug:
            print(f"\n  maintext contains cookies: {sum(maintext_describes_cookies_agreement_mask)}")
            print(f"  valid description: {sum(description_is_valid)}")
            print(f"  Before clearing cookies: {len(df)}")

        # salvage rows that have a valid description
        df.loc[(maintext_describes_cookies_agreement_mask & description_is_valid), 'maintext' ] = df.loc[(maintext_describes_cookies_agreement_mask & description_is_valid), 'description' ]

        # remove rows that have neither valid maintext nor valid description
        df = df.loc[~(maintext_describes_cookies_agreement_mask & ~description_is_valid)]
        
        if debug: print(f"  After clearing cookies: {len(df)}")
        return df