from extract.extract_channels import ext_channels

import traceback

try:
    ext_channels()

except:
    traceback.print_exc()
finally:
    pass