import string
from pathlib import Path
from typing import Type

import advertools as adv
import pandas as pd
import requests
from bs4 import BeautifulSoup

session = requests.Session()

def fetch_wftda():
    # Fetch data from the WFTDA website
    # and return it as a pandas DataFrame
    try:
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        wftda_df = pd.DataFrame()
        url = "https://resources.wftda.org/officiating/roller-derby-certification-program-for-officials/roster-of-certified-officials/"

        r = session.get(url=url)
        soup = BeautifulSoup(markup=r.text, features="lxml")
        rows = soup.find_all(name="h5")
        urls = [r.find("a")["href"] for r in rows]
        names = [r.find("a").get_text() for r in rows]
        wftda_df = pd.DataFrame(data={"Name": names, "url": urls})
        return wftda_df
    except Exception as e:
        print("Error fetching data: {}".format(e))
        return None
    
def fetch_twoevils():
    # Fetch data from the Two Evils website
    # and return it as a pandas DataFrame
    try:
        twoevils_df = pd.DataFrame()
        url = "https://twoevils.org/rollergirls/"
        twoevils_df = pd.read_html(io=url, skiprows=1)[0]
        twoevils_df.columns = [h.replace("Skater", "").strip() for h in twoevils_df.iloc[0]]
        twoevils_df = twoevils_df.rename(columns={"Date Added": "Registered"})
        twoevils_df = twoevils_df.iloc[1:-1, :].dropna(how="all")
        return twoevils_df
    except Exception as e:
        print("Error fetching data: {}".format(e))
        return None
    
def fetch_drc():
    # Fetch data from the Derby Roll Call website
    # and return it as a pandas DataFrame
    try:
        drc_url =  "http://www.derbyrollcall.com/everyone"
        drc_df = pd.read_html(io=drc_url)[0].rename(columns={"#": "Number"}).dropna(subset=["Name"])
        return drc_df
    except Exception as e:
        print("Error fetching data: {}".format(e))
        return None
    
def get_rdr_names(initial_letter):
    temp_names = []
    url = f"https://rollerderbyroster.com/view-names/?ini={initial_letter}"
    # print("Downloading names from {}".format(url))
    try:
        r = session.get(url=url)
        soup = BeautifulSoup(markup=r.text, features="lxml")
        rows = soup.find_all(name="ul")
        # Use only last unordered list - this is where names are!
        for idx, li in enumerate(iterable=rows[-1]):
            # Name should be the text of the link within the list item
            try:
                div = li.find("div", {"class": "search-info"})
                if div is None:
                    continue
                else:
                    name = div.find("a").get_text()
                    temp_names.append(name)
            except TypeError:
                pass
        rdr_df = pd.DataFrame(data={"Name": temp_names, "url": url})
        return rdr_df
    except requests.Timeout:
        print("Timeout!")
        pass


def fetch_rdr(letters=string.ascii_uppercase + string.digits):
    # Fetch data from the Roller Derby Roster website
    # and return it as a pandas DataFrame
    try:
        rdr_df = pd.DataFrame()
        for letter in letters:
            temp_df = get_rdr_names(initial_letter=letter)
            rdr_df = pd.concat(objs=[rdr_df, temp_df], ignore_index=True)
        return rdr_df
    except Exception as e:
        print("Error fetching data: {}".format(e))
        return None
    
def fetch_rdn_urls():
    # Fetch data from the Roller Derby Nations website
    # and return it as a pandas DataFrame
    try:
        # Get RDNation sitemap
        rdn_sitemap_url = "https://rdnation.com/sitemap.xml"
        rdn_sitemaps = adv.sitemap_to_df(sitemap_url=rdn_sitemap_url)
        # League pages have a specific URL structure
        rdn_sitemaps["is_league"] = (
            rdn_sitemaps["loc"].str.contains(pat="roller-derby-league/").fillna(False)
        )

        rdn_league_urls = sorted(
            rdn_sitemaps[
                rdn_sitemaps["is_league"]
                & (
                    rdn_sitemaps["loc"].str.contains(pat="/2/")
                    | rdn_sitemaps["loc"].str.contains(pat="/1/")
                )
            ]["loc"].tolist()
        )
        return rdn_league_urls
    except Exception as e:
        print("Error fetching data: {}".format(e))
        return None
    
def fetch_rdn_league(url):
    # Fetch data from a Roller Derby Nations league page
    # and return it as a pandas DataFrame
    try:
        rdn_league_df = pd.read_html(io=url)
        rdn_league_df = rdn_league_df.rename(columns={"Derby Name": "Name"})
        return rdn_league_df
    except Exception as e:
        print("Error fetching data: {}".format(e))
        return None
    
def fetch_rdn():
    # Fetch data from the Roller Derby Nations website
    # and return it as a pandas DataFrame
    try:
        rdn_df = pd.DataFrame()
        rdn_league_urls = fetch_rdn_urls()
        for url in rdn_league_urls:
            temp_df = fetch_rdn_league(url=url)
            rdn_df = pd.concat(objs=[rdn_df, temp_df], ignore_index=True)
        return rdn_df
    except Exception as e:
        print("Error fetching data: {}".format(e))
        return None