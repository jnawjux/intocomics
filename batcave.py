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