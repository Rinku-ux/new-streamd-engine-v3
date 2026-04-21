from PySide6.QtGui import QImageReader
print("Supported formats:", [fmt.data().decode() for fmt in QImageReader.supportedImageFormats()])
