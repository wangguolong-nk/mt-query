介绍：搜索query的高低质量分析，query的字面相似(编辑距离方法)，query的语义相似(word2vec方法)

1.把install/langconv.py和zh_wiki.py放在/usr/local/lib/python2.7/site-packages目录下

2.安装install/ChineseTone-0.1.4.zip

3.安装install/Pinyin2Hanzi-master.zip

4.安装python gensim

5.定时器

25 * * * * sh /home/wgl/script/deploy_scripts/bin/hour.start.sh

10 2 * * * sh /home/wgl/script/deploy_scripts/bin/day.start.sh

6.安装文件：install目录

