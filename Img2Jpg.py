from PIL import Image
import traceback
import io

def Img2Jpg(buf, dstFile):
    r"""
    图片转换大小
    """
    exMsg = ''
    try:
        srcImg = Image.open(io.BytesIO(buf))
        dstImg = srcImg.resize((108, 150), Image.ANTIALIAS).convert('RGB')
        dstImg.save(dstFile, 'JPEG')
    except:
        exMsg = '* ' + traceback.format_exc()
        print(exMsg)
    if exMsg:
        return False
    return True

if __name__ == "__main__":
    # Img2Jpg('1','2')     
