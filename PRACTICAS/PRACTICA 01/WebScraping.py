import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def scrape_and_download_pdfs(page_url, download_folder='pdfs', delay_seconds=1):
    """
    Scrapea una página y descarga todos los enlaces a archivos .PDF que encuentre.
    """
    # Crea la carpeta de destino si no existe
    os.makedirs(download_folder, exist_ok=True)

    # Solicita la página
    response = requests.get(page_url)
    response.raise_for_status()  # lanzar error si no respondió correctamente

    # Parseo HTML con BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Conjunto para evitar duplicados
    pdf_links = set()

    # Busca todos los enlaces (<a href=...>) que terminen en '.pdf'
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.lower().endswith('.pdf'):
            full_url = urljoin(page_url, href)  # convierte ruta relativa → absoluta
            pdf_links.add(full_url)

    print(f"Encontrados {len(pdf_links)} enlaces PDF en {page_url}.")

    # Descarga cada PDF uno por uno
    for idx, pdf_url in enumerate(pdf_links, start=1):
        filename = os.path.basename(pdf_url.split('?')[0])  # nombre limpio desde URL
        target_path = os.path.join(download_folder, filename)
        try:
            print(f"[{idx}/{len(pdf_links)}] Descargando: {pdf_url}")
            r = requests.get(pdf_url, stream=True, timeout=20)
            r.raise_for_status()
            with open(target_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Guardado: {target_path}")
        except Exception as e:
            print(f"Error al descargar {pdf_url}: {e}")
        time.sleep(delay_seconds)  # pausa entre descargas para no saturar el servidor

if __name__ == '__main__':
    page_to_scrape = 'https://resources.data.gov/categories/data-management-governance/'
    scrape_and_download_pdfs(page_to_scrape, download_folder='data_gov_pdfs', delay_seconds=1)
