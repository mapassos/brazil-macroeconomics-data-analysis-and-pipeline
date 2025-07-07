from __future__ import annotations
import os

DELIMITER = os.getenv('DELIMITER')

def scrap_selic() -> list[list[str]]:
    '''
    Extrai os dados da tabela contida no site do Banco Central do Brasil por meio de um browser headless.
    '''

    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    from selenium.webdriver.firefox.options import Options
    
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Firefox(options = options)
        
    url = "https://www.bcb.gov.br/controleinflacao/historicotaxasjuros"
    
    try:
        driver.get(url)
    except:
        driver.close()
        driver.quit()
    
    WebDriverWait(driver, 10).until(EC.presence_of_element_located(
        ('id', 'historicotaxasjuros')
        ))
    table = driver.find_element('id', 'historicotaxasjuros')
    rows = table.find_elements('tag name', "tr")
    while len(rows) < 3:
        rows = table.find_elements('tag name', "tr")

    content = []
    
    for row in rows:
        cols = row.find_elements('tag name', "td")
        if len(cols) > 0:
            content.append([])
            for col in cols:
                content[-1].append(col.text)

    driver.close()
    driver.quit()
    return content

def main() -> int:
    for line in scrap_selic():
        print(DELIMITER.join(line) + '\\n')
    return 0

if __name__ == '__main__':
    main()
