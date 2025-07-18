# DataComparer.py
import pandas as pd
import json
import logging


class DataComparer:
    def __init__(self, config_path, log_file):
        with open(config_path, 'r', encoding="utf-8") as f:
            self.config = json.load(f)

        logging.basicConfig(filename=log_file, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

        self.price_list = None
        self.website_list = None
        self.missing_in_price = set()
        self.missing_on_site = set()

        # Пути к файлам
        self.price_file_path = self.config.get("price_file_path")
        self.site_file_path = self.config.get("site_file_path")

        # Названия колонок
        columns = self.config.get("columns", {})
        self.price_column = columns.get("price_column", "Код")
        self.site_column = columns.get("site_column", "IE_XML_ID")
        self.secondary_column = columns.get("secondary_match_column", "IP_PROP2090")
        self.delete_col = columns.get("delete_column", "Удалить")
        self.stock_col = columns.get("stock_column", "Главный Склад")
        self.status_col = columns.get("status_column", "Статус")

        # Фильтрация
        self.filter_rules = self.config.get("filter_rules", {}).get("exclude_if", [])

        # Выходные файлы
        self.output_files = self.config.get("output_files", {
            "missing_on_site": "missing_on_site.xlsx",
            "missing_in_price": "missing_in_price.xlsx",
            "new_items": "new_items.xlsx"
        })

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

    def compare_data(self):
        logging.info('Comparing data')
        price_set = set(self.price_list[self.price_column])
        site_set = set(self.website_list[self.site_column])

        initial_missing_in_price = site_set - price_set
        initial_missing_on_site = price_set - site_set

        secondary_matches = 0
        final_missing_in_price = set()
        final_missing_on_site = set()

        if self.secondary_column not in self.website_list.columns:
            logging.warning(f"Column '{self.secondary_column}' not found. Skipping secondary comparison.")
            final_missing_in_price = initial_missing_in_price
            final_missing_on_site = initial_missing_on_site
        else:
            for item in initial_missing_in_price:
                site_row = self.website_list[self.website_list[self.site_column] == item]
                if not site_row.empty:
                    value = site_row[self.secondary_column].iloc[0]
                    if pd.notna(value):
                        codes = [code.strip() for code in value.split('+')]
                        found_match = all(any(code == p for p in price_set) for code in codes)
                        if found_match:
                            secondary_matches += 1
                        else:
                            final_missing_in_price.add(item)
                    else:
                        final_missing_in_price.add(item)
                else:
                    final_missing_in_price.add(item)

            for item in initial_missing_on_site:
                found = False
                for value in self.website_list[self.secondary_column].dropna():
                    codes = [code.strip() for code in value.split('+')]
                    if item in codes:
                        found = True
                        secondary_matches += 1
                        break
                if not found:
                    final_missing_on_site.add(item)

        self.missing_in_price = final_missing_in_price
        self.missing_on_site = final_missing_on_site

        logging.info(f'Initial mismatches: {len(initial_missing_in_price) + len(initial_missing_on_site)}')
        logging.info(f'Secondary matches found: {secondary_matches}')
        logging.info(f'Final missing in price: {len(self.missing_in_price)}')
        logging.info(f'Final missing on site: {len(self.missing_on_site)}')

    def apply_filters(self):
        logging.info('Applying filters')
        df = self.price_list[self.price_list[self.price_column].isin(self.missing_on_site)]

        base_condition = (df[self.delete_col] == "Нет") | df[self.delete_col].isna()
        exclusion_mask = pd.Series([False] * len(df), index=df.index)

        for rule in self.filter_rules:
            status_values = rule.get("status", [])
            stock_lt = rule.get("stock_less_than")
            stock_eq = rule.get("stock_equals")

            status_mask = df[self.status_col].isin(status_values)

            if stock_lt is not None:
                stock_mask = df[self.stock_col] < stock_lt
            elif stock_eq is not None:
                stock_mask = df[self.stock_col] == stock_eq
            else:
                stock_mask = pd.Series([True] * len(df), index=df.index)

            exclusion_mask |= (status_mask & stock_mask)

        filtered_df = df[base_condition & ~exclusion_mask]
        return filtered_df

    def save_results(self):
        logging.info('Saving results')

        filtered_df = self.apply_filters()
        filtered_df.to_excel(self.output_files["missing_on_site"], index=False)

        new_items_df = self.price_list[
            (self.price_list[self.price_column].isin(self.missing_on_site)) &
            (self.price_list[self.status_col] == "Новинка")
        ]
        if not new_items_df.empty:
            new_items_df.to_excel(self.output_files["new_items"], index=False)

        missing_in_price_df = self.website_list[
            self.website_list[self.site_column].isin(self.missing_in_price)
        ]
        missing_in_price_df.to_excel(self.output_files["missing_in_price"], index=False)

        logging.info(f'Results saved. Filtered: {len(filtered_df)}, New items: {len(new_items_df)}')

    def run(self):
        logging.info('Starting comparison')
        try:
            self.price_list = self.load_data(self.price_file_path)
            self.website_list = self.load_data(self.site_file_path)
            self.compare_data()
            self.save_results()
            logging.info('Comparison completed successfully')
        except Exception as e:
            logging.error(f"Error during comparison: {e}")


# Пример использования
# comparer = DataComparer('paths.json', 'app.log')
# comparer.run('price_file_path', 'site_file_path', 'Код', 'IE_XML_ID')
