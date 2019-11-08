from bs4 import BeautifulSoup
import requests
import utils

dic = {
    # 'Acoustics Research Letters Online': 'https://asa.scitation.org/toc/arl/current',
    'AIP Advances': 'https://aip.scitation.org/toc/adv/current',
    'AIP Conference Proceedings': 'https://aip.scitation.org/toc/apc/current',
    # 'American Journal of Physics': 'https://aapt.scitation.org/toc/ajp/current',
    'APL Bioengineering': 'https://aip.scitation.org/toc/apb/current',
    'APL Materials': 'https://aip.scitation.org/toc/apm/current',
    'APL Photonics': 'https://aip.scitation.org/toc/app/current',
    'Applied Physics Letters': 'https://aip.scitation.org/toc/apl/current',
    'Applied Physics Reviews': 'https://aip.scitation.org/toc/are/current',
    # 'Biointer phases': 'https://avs.scitation.org/toc/bip/current',
    'Biomicrofluidics': 'https://aip.scitation.org/toc/bmf/current',
    'Chaos: An Interdisciplinary Journal of Nonlinear Science': 'https://aip.scitation.org/toc/cha/current',
    # 'Chinese Journal of Chemical Physics': 'https://cps.scitation.org/toc/cjp/current',
    'Computers in Physics': 'https://aip.scitation.org/toc/cip/current',
    'Computing in Science & Engineering': 'https://aip.scitation.org/toc/csx/current',
    # 'JASA Express Letters': 'https://asa.scitation.org/topic/jasa-el',
    'Journal of Applied Physics': 'https://aip.scitation.org/toc/jap/current',
    'The Journal of Chemical Physics': 'https://aip.scitation.org/toc/jcp/current',
    # 'Journal of Laser Applications': 'https://lia.scitation.org/toc/jla/current',
    'Journal of Mathematical Physics': 'https://aip.scitation.org/toc/jmp/current',
    'Journal of Physical and Chemical Reference Data': 'https://aip.scitation.org/toc/jpr/current',
    'Journal of Renewable and Sustainable Energy': 'https://aip.scitation.org/toc/rse/current',
    # 'Journal of Rheology': 'https://sor.scitation.org/toc/jor/current',
    # 'The Journal of the Acoustical Society of America': 'https://asa.scitation.org/toc/jas/current',
    # 'Journal of Vacuum Science & Technology': 'https://avs.scitation.org/toc/jvs/current',
    # 'Journal of Vacuum Science & Technology A: Vacuum, Surfaces & Films': 'https://avs.scitation.org/toc/jva/current',
    # 'Journal of Vacuum Science & Technology B: Nanotechnology & Microelectronics: Materials, Processing, Measurement & Phenomena': 'https://avs.scitation.org/toc/jvb/current',
    'Low Temperature Physics': 'https://aip.scitation.org/toc/ltp/current',
    # 'Magnetism and Magnetic Materials': 'https://aip.scitation.org/topic/mmm',
    # 'Noise Control': 'https://asa.scitation.org/toc/noc/current',
    'Physics of Fluids': 'https://aip.scitation.org/toc/phf/current',
    'Physics of Plasmas': 'https://aip.scitation.org/toc/php/current',
    # 'The Physics Teacher': 'https://aapt.scitation.org/toc/pte/current',
    'Physics Today': 'https://physicstoday.scitation.org/toc/pto/current',
    # 'Proceedings of Meetings on Acoustics': 'https://asa.scitation.org/toc/pma/current',
    'Review of Scientific Instruments': 'https://aip.scitation.org/toc/rsi/current',
    # 'Scilight': 'https://aip.scitation.org/toc/sci/current',
    # 'Sound: Its Uses and Control': 'https://asa.scitation.org/toc/sou/current',
    'Structural Dynamics': 'https://aca.scitation.org/toc/sdy/current',
    # 'Surface Science Spectra': 'https://avs.scitation.org/toc/sss/current'
}
total = 0
conn = utils.init_db('mysql', 'aipjournal')
cur = conn.cursor()
stmt = 'insert ignore into issue(url,stat) Values(%s,%s)'
result = []
for index, each in enumerate(dic.keys()):
    print(index + 1, len(dic.keys()))
    catalog_response = requests.get(dic[each])
    soup = BeautifulSoup(catalog_response.text, 'lxml')
    volumes_urls = soup.find_all('li', class_='row js_issue')
    volumes_urls = [x.a.get('href') for x in volumes_urls]
    for every in volumes_urls:
        result.append((every, 0))
        if utils.parse_results_to_sql(conn, stmt, result, 1000):
            total += len(result)
            result.clear()
            print('插入 ', total, ' 个结果到数据库成功')
    utils.parse_results_to_sql(conn, stmt, result)
    total += len(result)
    result.clear()
    print('插入 ', total, ' 个结果到数据库成功')

print('complete')
