from bs4 import BeautifulSoup

def extract_text_from_html(html_path):
    with open(html_path, "r", encoding="utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")
    text = soup.get_text(separator="\n")
    return text

if __name__ == "__main__":
    html_path = "Bombardier_Inc_2023.html"
    texte = extract_text_from_html(html_path)

    for i, line in enumerate(texte.split("\n")):
        line = line.strip()
        if line:
            print(f"Ligne {i+1}: {line}")