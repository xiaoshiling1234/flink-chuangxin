package com.chuangxin.bean.api;

import lombok.Data;

@Data
public class PatentSearchExpressionPO extends BaseApiPO{
    String express="((名称+摘要和说明+权利要求书+说明书全文=无人机) OR (关键词=无人机) OR (技术领域=无人机) OR (背景技术=无人机) OR (发明内容=无人机) OR (具体实施方式=无人机) OR (附图说明=无人机))";
    String page="1";
    String sort_column="+PD";
    String exactSearch="1";
    String page_row="50";
}
