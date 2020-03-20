# SimpleDB
cs186-simpledb

## 概述
CS186-2013的homeworks,包括4个project,使用Java语言实现,由于年代原因用maven替代了ant。

## 日志

### 2020-03-20 01h00m
1. Tuple中的Field用普通数组替换了原List, 因为涉及get i和set i, 而List是动态大小。
2. 找到了使SanTest两个失败的原因，在于HeapFile中Iterator的实现有瑕疵，以为通过HeapFileReadTest后HeapFile就正确了。
3. 注意some_data_file.txt要多留一个空行
4. 跑通了pro1的整体流程。
