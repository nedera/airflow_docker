import re
def rename_cols(col):
    # Chuyển đổi tất cả các ký tự thành chữ thường
    col = col.lower()
    # Thay thế các dấu cách, /, - bằng dấu gạch dưới (_)
    col = re.sub(r'[\s/-]+', '_', col)
    # Viết hoa chữ cái đầu của từ
    col = re.sub(r'\b\w', lambda x: x.group().upper(), col)
    return col