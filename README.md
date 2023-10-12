# elasticmodel
Simple es bool search and query service

Elasticsearch工具类、PHP版基础ES搜索工具类
@todo其中包含了很多 冗余重复 代码可以抽象优化，为了 简单易懂 没有优化处理 <不喜勿喷>！！！

使用Demo：
include_once("elasticTools.php");
$size = 20;
$pageSize = 1;
$results = $esTools->setIndex('order');
  // must搜索
    // ->where('status', 2)
    // ->whereIn('shipping_status', array(1, 2))
    // ->match('consignee', '李大侠')
    // ->setNestedQuery('goods', 'term', 'goods.order_id', 1002, array('type' => 'must_not'))
  // filter过滤
    // ->filter('user_id', 2)
  // should搜索
    // ->orWhere('status', 2)
    // ->orderBy('create_time', 'desc')
  ->limit($size)
  ->offset(($pageSize - 1) * $size)
//    ->printQuery() // 打印query
    ->search();
