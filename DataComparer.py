# DataComparer.py
import pandas as pd
import json
import logging

class DataComparer:
    def __init__(self, paths_file, log_file):
        self.website_list = None
        self.price_list = None
        self.missing_in_price = None
        self.missing_on_site = None
        with open(paths_file, 'r', encoding="utf-8") as f:
            self.paths = json.load(f)
        logging.basicConfig(filename=log_file, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

    @staticmethod
    def load_data(file_path, separator=";", encoding="utf-8"):
        try:
            if file_path.endswith('.csv'):
                return pd.read_csv(file_path, sep=separator, encoding=encoding)
            elif file_path.endswith('.xlsx'):
                return pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path}")
        except Exception as e:
            logging.error(f"Error loading data from {file_path}: {e}")
            raise

    def compare_data(self, price_column_name, site_column_name):
        logging.info('Comparing data')
        price_set = set(self.price_list[price_column_name])
        website_set = set(self.website_list[site_column_name])

        # Первичная проверка по IE_XML_ID
        initial_missing_in_price = website_set - price_set
        initial_missing_on_site = price_set - website_set

        secondary_matches = 0
        final_missing_in_price = set()
        final_missing_on_site = set()

        # Проверяем наличие столбца IP_PROP2090
        has_ip_prop2090 = 'IP_PROP2090' in self.website_list.columns

        if not has_ip_prop2090:
            logging.warning("Column 'IP_PROP2090' is missing in website_list. Skipping secondary comparison.")
            # Вторичной проверки не будет
            final_missing_in_price = initial_missing_in_price
            final_missing_on_site = initial_missing_on_site
        else:
            # Проверяем товары, которых нет в прайсе
            for item in initial_missing_in_price:
                site_row = self.website_list[self.website_list[site_column_name] == item]

                if not site_row.empty:
                    ip_prop2090_value = site_row['IP_PROP2090'].iloc[0]

                    if pd.notna(ip_prop2090_value):
                        prop2090_codes = ip_prop2090_value.split('+')
                        prop2090_codes = [code.strip() for code in prop2090_codes]

                        found_match = all(any(price_code.strip() == prop_code for price_code in price_set)
                                          for prop_code in prop2090_codes)

                        if found_match:
                            secondary_matches += 1
                        else:
                            final_missing_in_price.add(item)
                    else:
                        final_missing_in_price.add(item)
                else:
                    final_missing_in_price.add(item)

            # Проверяем товары, которых нет на сайте
            for item in initial_missing_on_site:
                found = False
                for prop2090 in self.website_list['IP_PROP2090'].dropna():
                    if pd.notna(prop2090):
                        codes = prop2090.split('+')
                        if any(item.strip() == code.strip() for code in codes):
                            found = True
                            secondary_matches += 1
                            break

                if not found:
                    final_missing_on_site.add(item)

        self.missing_in_price = final_missing_in_price
        self.missing_on_site = final_missing_on_site

        logging.info(f'Initial mismatches: {len(initial_missing_in_price) + len(initial_missing_on_site)}')
        logging.info(f'Secondary matches found: {secondary_matches}')
        logging.info(f'Final missing in price list: {len(self.missing_in_price)} items')
        logging.info(f'Final missing on site: {len(self.missing_on_site)} items')

    def save_results(self, price_column_name, site_column_name):
        logging.info('Saving results')

        # Фильтрация missing_on_site по условиям
        missing_df = self.price_list[self.price_list[price_column_name].isin(self.missing_on_site)]

        # Основной фильтр
        filtered_df = missing_df[
            # Базовые условия
            ((missing_df['Удалить'] == 'Нет') | missing_df['Удалить'].isna()) &

            # Фильтр по статусам и остаткам
            ~(
                # Исключаем товары с остатком < 10 и определенными статусами
                    ((missing_df['Главный Склад'] < 10) &
                     missing_df['Статус'].isin(['Не указана', 'Перекуп', 'Снят с производства', 'Сборка'])) |

                    # Исключаем акционные товары с остатком < 5
                    ((missing_df['Статус'] == 'Акция') & (missing_df['Главный Склад'] < 5)) |

                    # Исключаем товары "Распродаем" или "Под заказ" с остатком < 10
                    ((missing_df['Статус'].isin(['Распродаем', 'Под заказ'])) &
                     (missing_df['Главный Склад'] < 10)) |

                    # Исключаем товары "На складе" с нулевым остатком
                    ((missing_df['Статус'] == 'На складе') & (missing_df['Главный Склад'] == 0))
            )
            ]

        # Сохраняем основной файл
        filtered_df.to_excel('missing_on_site.xlsx', index=False)

        # Сохраняем новинки в отдельный файл
        new_items_df = missing_df[missing_df['Статус'] == 'Новинка']
        if not new_items_df.empty:
            new_items_df.to_excel('new_items.xlsx', index=False)

        # Сохраняем missing_in_price без изменений
        missing_in_price_df = self.website_list[self.website_list[site_column_name].isin(self.missing_in_price)]
        missing_in_price_df.to_excel('missing_in_price.xlsx', index=False)

        logging.info(f'Results saved successfully. Filtered items: {len(filtered_df)}')
        if not new_items_df.empty:
            logging.info(f'New items found: {len(new_items_df)}')

    def run(self, price_file_path_key, site_file_path_key, price_column_name, site_column_name):
        logging.info('Starting comparison')
        try:
            self.price_list = DataComparer.load_data(self.paths[price_file_path_key])
            self.website_list = DataComparer.load_data(self.paths[site_file_path_key])
            self.compare_data(price_column_name, site_column_name)
            self.save_results(price_column_name, site_column_name)
            logging.info('Comparison completed successfully')
        except Exception as e:
            logging.error(f"Error during comparison: {e}")

# Пример использования
# comparer = DataComparer('paths.json', 'app.log')
# comparer.run('price_file_path', 'site_file_path', 'Код', 'IE_XML_ID')
