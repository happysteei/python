import os


def get_desktop_path() -> str:
    """获取用户桌面路径（跨平台支持Windows/Mac/Linux）"""
    return os.path.expanduser("~/Desktop")


def load_address_codes(filename: str) -> dict:
    """
    读取桌面地址码文件，解析为 {6位代码: 地区名称} 的字典
    文件格式要求：每行是 "110101=北京市东城区" 格式，支持空行
    """
    desktop = get_desktop_path()
    file_path = os.path.join(desktop, filename)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"桌面未找到文件：{filename}\n请检查文件名是否正确")

    addr_dict = {}
    # 尝试两种常见编码，避免中文乱码
    encodings = ["utf-8", "gbk"]

    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:  # 跳过空行
                        continue
                    if "=" not in line:
                        print(f"警告：第{line_num}行格式错误（缺少'='），已跳过：{line}")
                        continue

                    code, area = line.split("=", 1)
                    code = code.strip()
                    area = area.strip()

                    if len(code) != 6 or not code.isdigit():
                        print(f"警告：第{line_num}行地址码无效（需6位数字），已跳过：{code}")
                        continue

                    addr_dict[code] = area
            break  # 编码成功则跳出
        except UnicodeDecodeError:
            continue  # 编码失败则尝试下一种
    else:
        raise ValueError("文件编码读取失败，请确保文件是UTF-8或GBK编码")

    if not addr_dict:
        raise ValueError("文件中未找到有效的地址码数据")

    print(f"成功加载 {len(addr_dict)} 个地址码\n")
    return addr_dict


def calculate_check_digit(id17: str) -> str:
    """计算身份证第18位校验码（国标ISO 7064:1983.MOD 11-2）"""
    weights = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
    check_map = ['1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2']
    total = sum(int(d) * w for d, w in zip(id17, weights))
    return check_map[total % 11]


def reverse_idcard_prefix(known_6_18: str, addr_dict: dict) -> list:
    """
    反推身份证前5位
    :param known_6_18: 已知的身份证第6-18位（13位）
    :param addr_dict: 地址码字典 {6位代码: 地区名称}
    :return: 符合条件的结果列表，包含前5位、地址码、地区、完整身份证号
    """
    if len(known_6_18) != 13:
        raise ValueError("身份证6-18位必须是13位字符，请检查输入")

    # 解析已知部分
    addr_6th = known_6_18[0]  # 地址码第6位
    birth_part = known_6_18[1:9]  # 出生日期7-14位
    seq_part = known_6_18[9:12]  # 顺序码15-17位
    target_check = known_6_18[12]  # 目标校验码18位

    # 从字典中筛选第6位匹配的地址码
    valid_codes = [code for code in addr_dict if code.endswith(addr_6th)]
    if not valid_codes:
        raise ValueError(f"地址码文件中未找到第6位为 '{addr_6th}' 的有效数据")

    # 双重校验：快速同余过滤 + 完整算法验证
    result = []
    for code in valid_codes:
        # 第一步：快速同余校验（过滤90%无效数据）
        a1_a5 = [int(c) for c in code[:5]]
        fast_sum = sum(w * d for w, d in zip([7, 9, 10, 5, 8], a1_a5))
        if fast_sum % 11 != 9:
            continue

        # 第二步：完整校验码验证（确保100%准确）
        full_id17 = code + birth_part + seq_part
        calc_check = calculate_check_digit(full_id17)
        if calc_check == target_check:
            result.append({
                "前5位": code[:5],
                "完整6位地址码": code,
                "对应地区": addr_dict[code],
                "完整身份证号": full_id17 + calc_check
            })

    return result


# -------------------------- 主程序执行 --------------------------
if __name__ == "__main__":
    # ================== 请在此处修改配置 ==================
    FILENAME = "xxxxxx.txt"  # 替换为你桌面上的实际文件名（如：地址码.txt）
    KNOWN_PART = "1200604275529"  # 已知的身份证6-18位
    # =====================================================

    try:
        # 1. 加载地址码文件
        addr_dict = load_address_codes(FILENAME)

        # 2. 执行反推
        result = reverse_idcard_prefix(KNOWN_PART, addr_dict)

        # 3. 输出结果
        if result:
            print(f"✅ 找到 {len(result)} 组符合条件的结果：")
            for idx, item in enumerate(result, 1):
                print(f"\n--- 结果 {idx} ---")
                print(f"前5位：{item['前5位']}")
                print(f"完整地址码：{item['完整6位地址码']}")
                print(f"对应地区：{item['对应地区']}")
                print(f"完整身份证号：{item['完整身份证号']}")
        else:
            print("❌ 未找到符合条件的结果，请检查：")
            print("   1. 已知的身份证6-18位是否输入正确")
            print("   2. 地址码文件是否包含出生当年的有效区划")

    except Exception as e:
        print(f"❌ 错误：{e}")