import requests
from bs4 import BeautifulSoup
import math
import psycopg2
from collections import deque
from threading import Thread
import time

# db configuration #
conn = psycopg2.connect(dbname='StudyingDB', user='postgres',
                        password='1234', host='localhost', port="5432")
cursor = conn.cursor()


# db configuration end #

# model classes #
class Category:
    def __init__(self, name, category_id, count):
        self.name = name
        self.category_id = category_id
        self.count = count

    def to_string(self):
        return f"{self.name}: {self.category_id} | {self.count}"


class Entity:
    def __init__(self, entity_name, attributes, category):
        self.entity_name = entity_name
        self.attributes = attributes
        self.category = category

    def to_string(self):
        s = f"${self.entity_name}: "
        for attr in self.attributes:
            s += f"{attr.attribute_name}={attr.value} | "
        return s


class Attribute:
    def __init__(self, attribute_name, value):
        self.attribute_name = attribute_name
        self.value = value


# model classes block ended #

# Db Executor class #


class DbExecutor:
    def __init__(self):
        self.deque = deque()
        self.isAlive = True
        following_thread = Thread(target=self.follow_deque_updates)
        following_thread.start()

    def follow_deque_updates(self):
        while (self.isAlive):
            if len(self.deque) != 0:
                entity = self.deque.pop()
                self.add_entity_to_db(entity)
            else:
                time.sleep(0.2)
        pass

    def add_entity_to_db(self, entity):
        if entity.category.name in category_map:
            cat_id = category_map.get(entity.category.name)
        else:
            cursor.execute("INSERT INTO eav_category(name) VALUES(%s) RETURNING id", (entity.category.name,))
            cat_id = cursor.fetchone()[0]
            category_map[entity.category.name] = cat_id
            conn.commit()

        cursor.execute("INSERT INTO eav_entity_item(name, category_id) VALUES (%s, %s) RETURNING item_id",
                       (entity.entity_name, cat_id))
        ent_id = cursor.fetchone()[0]
        conn.commit()

        for attribute in entity.attributes:
            self.update_values_table(ent_id, attribute)

    def update_values_table(self, ent_id, attribute: Attribute):
        if attribute.attribute_name in attributes_map:
            attr_id = attributes_map[attribute.attribute_name]
        else:
            cursor.execute("INSERT INTO eav_attribute_item(name) VALUES(%s) RETURNING attribute_id",
                           (attribute.attribute_name,))
            attr_id = cursor.fetchone()[0]
            attributes_map[attribute.attribute_name] = attr_id
            conn.commit()

        cursor.execute("INSERT INTO eav_value_item(entity_id, attribute_id, value) values(%s, %s, %s)",
                       (ent_id, attr_id, attribute.value))
        conn.commit()

    def add_entity_to_queue(self, entity):
        self.deque.append(entity)


# Db Executor class #

# get map: attr_name/entity_name to attr_id/entity_id block #


def get_attributes_map():
    cursor.execute("SELECT attribute_id, name from eav_attribute_item")
    records = cursor.fetchall()
    attrs = {}
    for row in records:
        attr_id = row[0]
        attr_name = row[1]
        attrs[attr_name] = attr_id

    return attrs


def get_category_map():
    cursor.execute("SELECT id, name from eav_category")
    records = cursor.fetchall()
    cts = {}
    for row in records:
        ct_id = row[0]
        ct_name = row[1]
        cts[ct_name] = ct_id

    return cts


# get map: attr_name/entity_name to attr_id/entity_id block ended #


# global values #
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-'
}
IMG_ATTR_NAME = "Карточка товара"
TITLE_ATTR_NAME = "Название товара"
COST_ATTR_NAME = "Цена"
CATEGORY_ATTR_NAME = "Категория"
category_map = get_category_map()
attributes_map = get_attributes_map()
db_executor = DbExecutor()


# global values block ended #


def do_task():
    mvideo_url = "https://www.mvideo.ru/promo/akciya-rassrochka-mark199208912"
    req = requests.get(mvideo_url, headers=HEADERS)
    html_content = req.text

    soup = BeautifulSoup(html_content, "html.parser")
    category_block = soup.find("div", class_="c-plp-heading-facets")
    categories_elements = category_block.find("ul").find_all("li")

    categories = []
    for (i, cat_element) in enumerate(categories_elements):
        if i == 0:
            continue

        cat_link = cat_element.find("a")
        span_elems = cat_link.find_all("span")
        categories.append(
            Category(span_elems[0].text.strip(), cat_link.get("href").split("=")[-1], int(span_elems[1].text)))

    categories.sort(key=lambda x: x.count)
    get_info_about_categories(categories)


def get_info_about_categories(categories):
    items_per_page = 12
    for category in categories:
        for page in range(1, math.ceil(category.count / items_per_page) + 1):
            try:
                items_on_page(category, page)
            except Exception as ex:
                print(f"[+] Ooops... Exception: {ex}. Cat - {category.name}, page - {page}")
        print(f"[-------I finished {category.name} category-------]")


def items_on_page(category, page):
    print(f"I'm find: {category.name} page: {page}")
    items_url = (
        f"https://www.mvideo.ru/promo/akciya-rassrochka-mark199208912/f/page={page}?categoryId={category.category_id}"
        f"&&&sorting=true&prevActionType=changePage&prevActionValue=3")
    req = requests.get(items_url, headers=HEADERS, timeout=5)
    soup = BeautifulSoup(req.text, "html.parser")
    items_section = soup.find("div", attrs={"data-init": "productTileList"})
    items = items_section.find_all("div", class_="c-product-tile")

    for item in items:
        item_id = item.get("data-product-id")
        img_link = f"https://img.mvideo.ru/Pdb/{item_id}b.jpg"
        item_title = item.find("a", class_="fl-product-tile-title__link").text.strip()
        cost = item.find("span", class_="fl-product-tile-price__current").text.strip()

        item_attributes = item.find("div", class_="fl-product-tile-features").find_all("div",
                                                                                       class_="fl-product-tile-features__item")
        attrs = [Attribute(IMG_ATTR_NAME, img_link), Attribute(CATEGORY_ATTR_NAME, category.name), Attribute(COST_ATTR_NAME, cost)]
        for attribute_item in item_attributes:
            attr_name = attribute_item.find("span", class_="fl-product-tile-features__feature-name").text
            attr_value = attribute_item.find("span", class_='fl-product-tile-features__feature-value').text
            attrs.append(Attribute(attr_name, attr_value))

        db_executor.add_entity_to_queue(Entity(item_title, attrs, category))


if __name__ == "__main__":
    do_task()
