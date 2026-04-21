@echo off
echo Starting Nuitka build (Updated with Icon and SQL)...
python -m nuitka ^
    --standalone ^
    --enable-plugin=pyside6 ^
    --include-package=duckdb ^
    --include-package=pandas ^
    --include-package=numpy ^
    --include-package=aiohttp ^
    --include-package=requests ^
    --include-data-dir=ui=ui ^
    --include-data-dir=core=core ^
    --include-data-dir=docs=docs ^
    --include-data-files=1004.sql=1004.sql ^
    --include-data-files=1011.sql=1011.sql ^
    --windows-icon-from-ico=icon.ico ^
    --windows-console-mode=disable ^
    --assume-yes-for-downloads ^
    --product-name="Streamd BI Native Engine" ^
    --product-version=2.1.1 ^
    --file-description="Streamd BI Local Analysis Tool" ^
    --company-name="Streamd Project" ^
    --output-dir=dist ^
    --remove-output ^
    main.py
echo Build finished.
