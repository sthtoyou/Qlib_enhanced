#  Copyright (c) Microsoft Corporation.
#  Licensed under the MIT License.

import fire
import sys
from pathlib import Path

# 添加tests目录到Python路径，以便导入我们修改的增强版
sys.path.insert(0, str(Path(__file__).parent.parent / "tests"))

from data import GetDataEnhanced as GetData


if __name__ == "__main__":
    fire.Fire(GetData)
