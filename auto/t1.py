#!/usr/bin/env python3
import pandas 
from pandaUtil_mysql_chain import mysql_chain
from pandaUtil_append import chunk_append,readMysql

def f_root():
    "ReadMysqlDb"
    # 工商股东表
    # 主键id,公司编码,公司名称,股东编码,股东类别编码,股东名称（冗余）,认缴金额（明细）,实缴金额（明细）,认缴的出资额,持股比例,数据来源,是否有效,同步标识,数据状态,创建时间,修改时间
    for chunk in readMysql(mysql_chain.db_aliyun)[
        "sy_cd_ms_sh_gs_shlist_new",
        "id,compCode,compName,shId,shTypeCode,shName,capital,actualCapital,investAmount,holdRatio,infoSource,isValid,sourceId,dataStatus,createTime,modifyTime",
        "",
    ].df_chunks:
        yield chunk
def f_save(df):
    "SaveExcel"
    print('save.df', df)
    with pandas.ExcelWriter("default.xlsx") as writer:
        df = df[[
            'id',
                'compCode',
                'compName',
                'shId',
                'shTypeCode',
                'shName',
                'capital',
                'actualCapital',
                'investAmount',
                'holdRatio',
                'infoSource',
                'isValid',
                'sourceId',
                'dataStatus',
                'createTime',
                'modifyTime',
        ]]
        df = df.set_axis(
        [
            'id',
                'compCode',
                'compName',
                'shId',
                'shTypeCode',
                'shName',
                'capital',
                'actualCapital',
                'investAmount',
                'holdRatio',
                'infoSource',
                'isValid',
                'sourceId',
                'dataStatus',
                'createTime',
                'modifyTime',
        ], axis=1)
        df.to_excel(writer, sheet_name="default", index=False)

if __name__ == '__main__':
    dfs = []
    for i, chunk in enumerate(f_root()):
        for proc in []:
            chunk = proc(chunk)
        dfs.append(chunk)
        if i >= 2:
            break
    df = pandas.concat(dfs, axis=0)
    f_save(df)