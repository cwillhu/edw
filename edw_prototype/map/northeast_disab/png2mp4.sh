
ffmpeg -r 1 -i perc_disab_%2d.png -vf pad="width=ceil(iw/2)*2:height=ceil(ih/2)*2" -vcodec libx264 -pix_fmt yuv420p ./perc_disab.mp4

