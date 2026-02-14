
# Ramazon taqvimi 2026 (Toshkent vaqti bilan)
# Boshqa viloyatlar uchun farqlar (daqiqa) qo'shiladi yoki alohida yoziladi.

RAMADAN_2026_TASHKENT = {
    "2026-02-19": {"bomdod": "05:54", "shom": "18:03"},
    "2026-02-20": {"bomdod": "05:53", "shom": "18:04"},
    "2026-02-21": {"bomdod": "05:52", "shom": "18:06"},
    "2026-02-22": {"bomdod": "05:50", "shom": "18:07"},
    "2026-02-23": {"bomdod": "05:49", "shom": "18:08"},
    "2026-02-24": {"bomdod": "05:47", "shom": "18:09"},
    "2026-02-25": {"bomdod": "05:46", "shom": "18:10"},
    "2026-02-26": {"bomdod": "05:45", "shom": "18:12"},
    "2026-02-27": {"bomdod": "05:43", "shom": "18:13"},
    "2026-02-28": {"bomdod": "05:42", "shom": "18:14"},
    "2026-03-01": {"bomdod": "05:40", "shom": "18:15"},
    "2026-03-02": {"bomdod": "05:39", "shom": "18:16"},
    "2026-03-03": {"bomdod": "05:37", "shom": "18:17"},
    "2026-03-04": {"bomdod": "05:35", "shom": "18:19"},
    "2026-03-05": {"bomdod": "05:34", "shom": "18:20"},
    "2026-03-06": {"bomdod": "05:32", "shom": "18:21"},
    "2026-03-07": {"bomdod": "05:31", "shom": "18:22"},
    "2026-03-08": {"bomdod": "05:29", "shom": "18:23"},
    "2026-03-09": {"bomdod": "05:27", "shom": "18:24"},
    "2026-03-10": {"bomdod": "05:26", "shom": "18:25"},
    "2026-03-11": {"bomdod": "05:24", "shom": "18:27"},
    "2026-03-12": {"bomdod": "05:22", "shom": "18:28"},
    "2026-03-13": {"bomdod": "05:21", "shom": "18:29"},
    "2026-03-14": {"bomdod": "05:19", "shom": "18:30"},
    "2026-03-15": {"bomdod": "05:17", "shom": "18:31"},
    "2026-03-16": {"bomdod": "05:15", "shom": "18:32"},
    "2026-03-17": {"bomdod": "05:14", "shom": "18:33"},
    "2026-03-18": {"bomdod": "05:12", "shom": "18:34"},
    "2026-03-19": {"bomdod": "05:10", "shom": "18:35"},
}

# Viloyatlar kesimida farqlar (daqiqa)
# Bu yerga foydalanuvchi tashlagan rasmga qarab to'g'irlash mumkin.
REGION_OFFSETS = {
    "toshkent": {"sah": 0, "ift": 0},
    "toshkent-shahri": {"sah": 0, "ift": 0},
    "andijan": {"sah": -13, "ift": -13},
    "namangan": {"sah": -10, "ift": -9},
    "fergana": {"sah": -11, "ift": -11},
    "gulistan": {"sah": 2, "ift": 4},
    "jizzakh": {"sah": 6, "ift": 6},
    "samarqand": {"sah": 10, "ift": 10},
    "qarshi": {"sah": 16, "ift": 16},
    "navoiy": {"sah": 19, "ift": 19},
    "bukhara": {"sah": 24, "ift": 24},
    "urgench": {"sah": 35, "ift": 35},
    "nukus": {"sah": 38, "ift": 38},
    "termez": {"sah": 8, "ift": 8}, # Termiz farqi faslga qarab o'zgaradi, aniqlashtirish kerak
}

def get_ramadan_times(region_slug, date_str):
    """
    Berilgan sana va va hudud uchun saharlik va iftorlik vaqtini qaytaradi.
    Agar aniq jadval bo'lmasa, Toshkent vaqtiga offset qo'shib hisoblaydi.
    """
    if date_str not in RAMADAN_2026_TASHKENT:
        return None
    
    base_times = RAMADAN_2026_TASHKENT[date_str]
    offset = REGION_OFFSETS.get(region_slug, {"sah": 0, "ift": 0})
    
    # Vaqtlarni hisoblash
    def calc_time(time_str, delta_min):
        if not time_str: return "00:00"
        try:
            h, m = map(int, time_str.split(':'))
            total_min = h * 60 + m + delta_min
            new_h = (total_min // 60) % 24
            new_m = total_min % 60
            return f"{new_h:02d}:{new_m:02d}"
        except:
            return time_str

    return {
        "bomdod": calc_time(base_times["bomdod"], offset["sah"]),
        "shom": calc_time(base_times["shom"], offset["ift"])
    }
