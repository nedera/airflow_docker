import re
def rename_cols(col):
    col=col.lower()
    # Viết hoa chữ cái đầu của từ
    col = re.sub(r'\b(\w)|[-/](\w)', lambda x: x.group().upper(), col)
    # Thay thế các dấu cách, /, - bằng dấu gạch dưới (_)
    col = re.sub(r'[\s/-]+', '', col)
    return col