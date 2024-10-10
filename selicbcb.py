def main() -> int:
    browser = input('Digite qual dos dois browser a seguir será utilizado (Firefox/Chrome) ')
    data = retrieve_selic(browser)

    try:
        with open('dados/selic.csv') as old_file:
            old_data = old_file.readlines()
    except:
        old_data = []
    if len(data) > len(old_data):
        save_to_csv(data)
        print(f'Os dados foram salvos!')
    else:
        print(f'Sem dados novos!')
    input('Precione enter para finalizar.')
    return 0

def save_to_csv(data_list: list):
    import csv
    
    with open('dados/selic.csv', 'w', newline='') as csv_file:
        write = csv.writer(csv_file, delimiter= ';')
        write.writerows(data_list)
    print('Salvando como selic.csv')

def retrieve_selic(browser: str|None = None) -> list[list]:
    '''
    Extrai os dados da tabela contida no site do Banco Central do Brasil por meio de um browser headless.

    Parameters
    ----------
    browser : str
        Navegador a ser utilizado. Pode ser Chrome ou Firefox. Padrão = Firefox.

    Returns
    -------
    content: list(list)
        Conteúdo da tabela dinâmica.
    '''
    from selenium import webdriver
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    if browser:
        browser = browser.lower()
    
    if browser == 'firefox' or browser is None:
        from selenium.webdriver.firefox.options import Options
    
        options = Options()
        options.add_argument('--headless')
        driver = webdriver.Firefox(options = options)
        
    elif browser == 'chrome':
        from selenium.webdriver.chrome.options import Options
    
        options = Options()
        options.add_argument('--headless=old')
        driver = webdriver.Chrome(options = options)

    else:
        print('Opção inválida. Escolha entre firefox ou chrome')
    
    url = "https://www.bcb.gov.br/controleinflacao/historicotaxasjuros"
    
    try:
        driver.get(url)
    except:
        print('Problema com a conexão')
    
    WebDriverWait(driver, 10).until(EC.presence_of_element_located(('id', 'historicotaxasjuros')))
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

if __name__ == '__main__':
    main()
