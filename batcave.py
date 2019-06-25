from selenium.webdriver import Chrome

def get_amazon_list_ids(link):
    """Scraping function using Selenium for getting Amazon product IDs (ASIN) from 'Best Seller' pages.
    Args:
      link: html link of 'Best Sellers' page. 
    Returns:
      all_ids: a list of ASIN ids found on page. 
    """
    # Instantiate Chrome and open to link
    browser = Chrome()
    browser.get(link)

    # Grabs the links from the page, then seperates out the ASIN from the link for each product
    all_ids = [x.get_attribute('href').split('dp/')[1].split('/')[0] 
        for x in browser.find_elements_by_xpath('//*[@id="zg-ordered-list"]/li/span/div/span/a')]

    return all_ids


def get_related_ids(df):
    """Function to unpack a list of the unique related product ASINs from the 'Also bought' section.
    Args:
        df: Pandas dataframe (specifically from metadata.json as it works with its schema) 
    Returns:
        all_unique_ids: Python list of unique ASINs from a dataframe that are in the 'related' field
    """

    # Get each item from the sublist (related->also bought) and add to one list if not empty
    all_ids = [val for meta in df.related.tolist() \
                        for val in meta[0] if meta[0] is not None]

    # Condense to a list of unique ids to eliminate any overlap
    all_unique_ids = list(set(all_ids))

    return all_unique_ids
