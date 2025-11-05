import flet as ft
import time


# כאן אתה שם את הפונקציות/קבצים של כל גרסה
def run_version(version_func, filename):
    start = time.perf_counter()
    version_func(filename)
    end = time.perf_counter()
    return end - start


def main(page: ft.Page):
    page.title = "השוואת מהירות גרסאות פרסור"

    results = [ft.Text(f"גרסה {i + 1}: ממתין...") for i in range(4)]
    page.add(ft.Column(results))

    def run_all(_):
        fn_list = [parse_v1, parse_v2, parse_v3, parse_v4]  # הפונקציות שלך
        filename = "yourfile.bin"
        for i, fn in enumerate(fn_list):
            results[i].value = f"גרסה {i + 1}: רץ..."
            page.update()
            elapsed = run_version(fn, filename)
            results[i].value = f"גרסה {i + 1}: {elapsed:.2f} שניות"
            page.update()

    run_btn = ft.ElevatedButton(text="הרץ הכל", on_click=run_all)
    page.add(run_btn)


ft.app(target=main)
