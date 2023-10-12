<?php
require 'vendor/autoload.php';

use Elasticsearch\ClientBuilder;

/**
 * Elasticsearch工具类
 * @desc PHP版基础ES搜索工具类
 *  其中包含了很多 冗余重复 代码可以抽象优化，为了 简单易懂 没有优化处理 <不喜勿喷>！！！
 *  completion suggester 自动补全建议搜索，前缀搜索，使用Suggest一类方法的时候必须确保您的索引映射中有一个字段是 completion 类型，completion类型创建如下：
 *  {
        "mappings": {
            "properties": {
                "suggest": {
                    "type": "completion"
                }
            }
        }
    }
 * @author jianhaofly@163.com
 * @time 2023/09/21 09:22
 */
class elasticTools {

    private static $instance = null;// 单例对象
    private $client;                // 连接
    private $index;                 // 索引名称
    private $fields = [];           // 搜索字段
    private $conditions = [];       // 搜索条件
    private $sortFields = [];       // 排序字段
    private $isDistance = FALSE;    // 是否地理位置搜索
    private $isPrintQuery= FALSE;   // 是否打印Query 排查问题
    private $minimumShouldMatch = 1;// 满足查询条件所需的 should 子句的最小数量
    private $from = 0;
    private $size = 10;

    // 构造私有化，防止直接创建对象
    private function __construct() {
        // TODO: ES主机和端口
        $params = array(
            'hosts' => ['localhost:9200'],
        );
        $this->client = ClientBuilder::create()->setHosts($params['hosts'])->build();
    }

    // 公有静态方法，返回该类的唯一实例
    public static function getInstance() {
        if (is_null(self::$instance)) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    // 阻止对象被克隆
    private function __clone() {}

    // 阻止对象被反序列化
    private function __wakeup() {}

    /**
     * 设置索引
     * @param string $index
     * @return $this
     * @author gengjianhao
     * @time 2023/09/30 11:10
     */
    public function setIndex($index) {
        $this->index = $index;
        return $this;
    }

    /**
     * 设置满足查询条件所需的 should 子句的最小数量
     * @param int $num
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20/15:05
     */
    public function setMinimumShouldMatch($num) {
        $this->minimumShouldMatch = $num;
        return $this;
    }

    /**
     * 搜索单条数据
     * @params int $docId
     * @params string $primaryKey 必须是唯一标识
     * @return array|callable
     * @author gengjianhao
     * @time 2023/09/20 11:38
     */
    public function find($docId, $primaryKey = 'id') {
        // 文档ID不能为空
        if (empty($docId)) return array('code' => -1, 'message' => 'No docId specified', 'data' => array());

        // 索引不能为空
        if (empty($this->index)) return array('code' => -1, 'message' => 'No index specified', 'data' => array());

        // 查询结构体
        $params = [
            'index' => $this->index,
            'type'  => '_doc',
            'body'  => array(
                'query' => array(
                    'bool' => array(
                        'must' => array(
                            'term' => array(
                                $primaryKey => $docId
                            )
                        )
                    )
                )
            )
        ];

        try {
            $response = $this->client->search($params);
            if (isset($response['hits']['total']['value']) && $response['hits']['total']['value'] > 0) {
                return array('code' => 0, 'message' => '', 'data' => $response['hits']['hits'][0]['_source']);
            } else {
                return array('code' => 0, 'message' => 'no matching data', 'data' => array());
            }
        } catch (Exception $e) {
            return array('code' => 0, 'message' => $e->getMessage(), 'data' => array());
        }
    }

    /**
     * 查询字段列表
     * @params string $fields
     *  value: '' 只查询id
     *         'user'、'goods'、'order'自定义表字段
     *         'user_id,user_name.....'自定义搜索字段
     * @return array
     * @author gengjianhao
     * @time 2023/09/30 11:10
     */
    public function fieldList($fields = '') {
        switch ($fields) {
            case '':
                $this->fields = array('id');
                break;
            case 'user':
            case 'goods':
            case 'order':
            // 等等需要查询哪些字段单独处理

                break;
            default:
                // 指定字段查询
                $this->fields = explode(',', $fields);
        }
    }

    /**
     * 搜索多条数据
     * @params bool $isPrintQuery
     * @return array|callable
     * @author gengjianhao
     * @time 2023/09/30 11:10
     */
    public function search($isPrintQuery = false) {
        // 组装查询参数
        $searchParams = [
            'index' => $this->index,
            'type'  => '_doc',
            // 使用 dfs_query_then_fetch 可以确保更准确的相关性评分，因为它考虑了全局的文档频率。但这会带来额外的开销，因为需要先进行一个DFS阶段,大多数情况下，默认的 query_then_fetch 就足够好如果你发现由于文档分布不均或查询词条非常罕见而导致的相关性评分问题，那么可以考虑使用 dfs_query_then_fetch
            // 'search_type' => 'dfs_query_then_fetch',
            'client' => array(
                'future' => 'lazy',
                'ignore' => [400, 404]
            ),
            '_source' => $this->fields,
            'body'  => [
                // track_total_hits 用于控制是否完全计算总命中数,配置控制搜索结果中总命中数的跟踪的参数,大型数据集中提高搜索性能
                // 可传递参数：
                //  true (默认值)：这将返回搜索结果的精确总命中数
                //  false：不跟踪总命中数。这将返回一个总命中数，但这个数值是不精确的，并且仅仅是一个上界估计
                //  整数，假设设置 track_total_hits 为 10000，Elasticsearch 会跟踪并返回精确的总命中数，直到该数值达到 10000。一旦超过这个数值，返回的总命中数就会变成一个不精确的估计值
                'track_total_hits' => TRUE
            ]
        ];
        // 组装条件
        $searchParams['body']['query'] = array('must' => array('match_all' => new stdClass()));
        if (!empty($this->conditions)) {
            // 构造查询语句
            $query = $this->buildBoolQuery($this->conditions);

            // 如果包含should搜索需要设置minimum_should_match值
            if (isset($query['bool']['should'])) {
                $query['bool']['minimum_should_match'] = $this->minimumShouldMatch;
            }

            $searchParams['body']['query'] = $query;
        }
        // 排序
        if (!empty($this->sortFields)) {
            $searchParams['body']['sort'] = $this->sortFields;
        }
        // 分页
        $searchParams['body']['from'] = $this->from;
        $searchParams['body']['size'] = $this->size;

        // 打印格式化query
        if ($this->isPrintQuery) {
             echo "<pre>";
             var_dump(json_encode($searchParams));
             die;
        }

        // 查询结果集
        $response = $this->client->search($searchParams);
        // 清空查询参数，以便下次使用
        $this->conditions = [];

        //处理结果集
        $hitsRes = $res = array();
        $data = array('data' => array(), 'total' => 0);
        if (!empty($response['hits'])) $hitsRes = $response['hits'];
        if (empty($hitsRes) || $hitsRes['total']['value'] < 1) return $data;

        // 处理结果集
        foreach ($hitsRes['hits'] as $key => $val) {
            $tempData = $val['_source'];
            if ($this->isDistance && !empty($val['sort'])) {
                $distanceMetre = $val['sort'][0];
                $distanceKm = number_format($distanceMetre/1000,1);
                if ($distanceKm > 1) {
                    $tempData['distance'] = "{$distanceKm}km";
                } else {
                    $distanceKm = round($distanceMetre);
                    $tempData['distance'] = "{$distanceKm}m";
                }
            }
            $res[$key] = $tempData;
        }
        return array('data' => $res, 'total' => $hitsRes['total']['value']);
    }

    /**
     * 设置打印Query语句
     * @return $this
     * @author gengjianhao
     * @time 2023/09/22 17:10
     */
    public function printQuery() {
        $this->isPrintQuery = TRUE;
        return $this;
    }

    /**
     * 生成bool查询
     * @param array $params
     * @return array
     * @author gengjianhao
     * @time 2023/09/20 10:34
     */
    private function buildBoolQuery(array $conditions) {
        $query = [];

        foreach ($conditions as $type => $condition) {
            switch ($type) {
                case 'term':
                case 'match':
                case 'match_phrase':
                case 'terms':
                case 'range':
                    foreach ($condition as $field => $value) {
                        $query['must'][] = [$type => [$field => $value]];
                    }
                    break;
                case 'should':
                case 'must_not':
                    foreach ($condition as $searchType => $clauses) {
                        foreach ($clauses as $field => $value) {
                            $query[$type][] = [$searchType => [$field => $value]];
                        }
                    }
                    break;
                case 'filter':
                    $query['filter'] = $condition;
                    break;
                case 'nested':
                    foreach ($condition as $value) {
                        $parentType = $value['nested']['parent_type'];
                        unset($value['nested']['parent_type']);
                        $query[$parentType][] = ['nested' => $value['nested']];
                    }
                    break;
            }
        }

        return ['bool' => $query];
    }


    /*************************************** Must搜索 **************************************/
    /**
     * Term精确查找
     * @param string $field
     * @param string|int $value
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function where($field, $value) {
        $this->conditions['term'][$field] = $value;
        return $this;
    }

    /**
     * Terms WhereIn查询
     * @param string $field
     * @param array $values
     * @return $this
     * @example
     *  $esTools->whereIn('tags', ['hot', 'top'])
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function whereIn($field, array $values) {
        $this->conditions['terms'][$field] = $values;
        return $this;
    }

    /**
     * Range 区间查询
     * @param string $field
     * @param string $operator
     * @param string|int $leftVal
     * @param string|int $rightVal
     * @return $this
     * @example
     *  $esTools->whereRange('price', '<=>', 10, 50)
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function range($field, $operator, $leftVal, $rightVal = null) {
        if (!isset($this->conditions['range'][$field])) {
            $this->conditions['range'][$field] = [];
        }

        switch ($operator) {
            case '<':
                $this->conditions['range'][$field]['lt'] = $leftVal;
                break;
            case '<=':
                $this->conditions['range'][$field]['lte'] = $leftVal;
                break;
            case '>':
                $this->conditions['range'][$field]['gt'] = $leftVal;
                break;
            case '>=':
                $this->conditions['range'][$field]['gte'] = $leftVal;
                break;
            case '<=>':
                $this->conditions['range'][$field] = [
                    'gte' => $leftVal,
                    'lte' => $rightVal
                ];
                break;
        }

        return $this;
    }

    /**
     * Match 模糊查找
     * @param string $field
     * @param string $value
     * @return $this
     * @example
     *  $esTools->match('title', '今天 我的') 会调用match（带空格）
     *  $esTools->match('title', '今天我的') 会调用match_phrase
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function match($field, $value) {
        $matchParams = $this->_handleMatch($field, $value);
        list($searchType, $searchParam) = array($matchParams['type'], $matchParams['params']);

        $this->conditions[$searchType] = $searchParam;
        return $this;
    }


    /*************************************** Should搜索 **************************************/
    /**
     * Should Term精确查找
     * @param string $field
     * @param string|int $value
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function orWhere($field, $value) {
        $this->conditions['should']['term'][$field] = $value;
        return $this;
    }

    /**
     * Should Wherein查询
     * @param string $field
     * @param array $values
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function orWhereIn($field, array $values) {
        $this->conditions['should']['terms'][$field] = $values;
        return $this;
    }

    /**
     * Should Match匹配查找
     * @param string $field
     * @param string $value
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function orMatch($field, $value) {
        $matchParams = $this->_handleMatch($field, $value);
        list($searchType, $searchParam) = array($matchParams['type'], $matchParams['params']);

        $this->conditions['should'][$searchType] = $searchParam;
        return $this;
    }

    /**
     * Should Range查询
     * @param string $field
     * @param string $operator
     * @param string|int $leftVal
     * @param string|int $rightVal
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function orRange($field, $operator, $leftVal, $rightVal = null) {
        if (!isset($this->conditions['should']['range'][$field])) {
            $this->conditions['should']['range'][$field] = [];
        }

        switch ($operator) {
            case '<':
                $this->conditions['should']['range'][$field]['lt'] = $leftVal;
                break;
            case '<=':
                $this->conditions['should']['range'][$field]['lte'] = $leftVal;
                break;
            case '>':
                $this->conditions['should']['range'][$field]['gt'] = $leftVal;
                break;
            case '>=':
                $this->conditions['should']['range'][$field]['gte'] = $leftVal;
                break;
            case '<=>':
                $this->conditions['should']['range'][$field] = [
                    'gte' => $leftVal,
                    'lte' => $rightVal
                ];
                break;
        }

        return $this;
    }


    /*************************************** MustNot过滤搜索 **************************************/
    /**
     * MustNot Term精确匹配
     * @param string $field
     * @param string|int $value
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function notWhere($field, $value) {
        $this->conditions['must_not']['term'][$field] = $value;
        return $this;
    }

    /**
     * MustNot Match匹配查找
     * @param string $field
     * @param string $value
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function notMatch($field, $value) {
        $matchParams = $this->_handleMatch($field, $value);
        list($searchType, $searchParam) = array($matchParams['type'], $matchParams['params']);

        $this->conditions['must_not'][$searchType] = $searchParam;
        return $this;
    }

    /**
     * MustNot Wherein查询
     * @param string $field
     * @param array $values
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function notWhereIn($field, array $values) {
        $this->conditions['must_not']['terms'][$field] = $values;
        return $this;
    }

    /**
     * MustNot Range查询
     * @param string $field
     * @param string $operator
     * @param string|int $leftVal
     * @param string|int $rightVal
     * @return $this
     * @author gengjianhao
     * @time 2023/09/20 10:01
     */
    public function notRange($field, $operator, $leftVal, $rightVal = null) {
        if (!isset($this->conditions['must_not']['range'][$field])) {
            $this->conditions['must_not']['range'][$field] = [];
        }

        switch ($operator) {
            case '<':
                $this->conditions['must_not']['range'][$field]['lt'] = $leftVal;
                break;
            case '<=':
                $this->conditions['must_not']['range'][$field]['lte'] = $leftVal;
                break;
            case '>':
                $this->conditions['must_not']['range'][$field]['gt'] = $leftVal;
                break;
            case '>=':
                $this->conditions['must_not']['range'][$field]['gte'] = $leftVal;
                break;
            case '<=>':
                $this->conditions['must_not']['range'][$field] = [
                    'gte' => $leftVal,
                    'lte' => $rightVal
                ];
                break;
        }

        return $this;
    }


    /*************************************** Filter过滤搜索 **************************************/
    /**
     * Filter Term过滤
     * @param string $field
     * @param string|int $value
     * @return $this
     * @author gengjianhao
     * @time 2023/09/21 11:49
     */
    public function filter($field, $value) {
        $this->conditions['filter'][] = [
            'term' => [$field => $value]
        ];
        return $this;
    }

    /**
     * Filter Terms过滤
     * @param string $field
     * @param array $values
     * @return $this
     * @example
     *  $esTools->filterTerms('categories', ['electronics', 'computing'])
     * @author gengjianhao
     * @time 2023/09/21 15:21
     */
    public function filterTerms($field, array $values) {
        $this->conditions['filter'][] = [
            'terms' => [$field => $values]
        ];
        return $this;
    }

    /**
     * Filter Range过滤
     * @param string $field
     * @param string $operator
     * @param $leftVal
     * @param $rightVal
     * @return $this
     * @example
     *  $esTools->filterRange('price', '<=>', 55, 120);
     * @author gengjianhao
     * @time 2023/09/21 15:25
     */
    public function filterRange($field, $operator, $leftVal, $rightVal = null) {
        if (!isset($this->conditions['filter']['range'][$field])) {
            $this->conditions['filter']['range'][$field] = [];
        }

        switch ($operator) {
            case '<':
                $this->conditions['filter']['range'][$field]['lt'] = $leftVal;
                break;
            case '<=':
                $this->conditions['filter']['range'][$field]['lte'] = $leftVal;
                break;
            case '>':
                $this->conditions['filter']['range'][$field]['gt'] = $leftVal;
                break;
            case '>=':
                $this->conditions['filter']['range'][$field]['gte'] = $leftVal;
                break;
            case '<=>':
                $this->conditions['filter']['range'][$field] = [
                    'gte' => $leftVal,
                    'lte' => $rightVal
                ];
                break;
        }

        return $this;
    }

    /**
     * Filter 地理位置过滤
     * @param string $field
     * @param string $lat
     * @param string $lon
     * @param int $distance 距离（格式：1km/1000m）
     * @return $this
     * @example
     *  $esTools->filterGeoDistance('location', 40.730610, -73.935242, '10km')
     * @author gengjianhao
     * @time 2023/09/20 15:07
     */
    public function filterGeoDistance($field, $lat, $lon, $distance) {
        $this->conditions['filter'][] = [
            'geo_distance' => [
                'distance' => $distance,
                $field => [
                    'lat' => $lat,
                    'lon' => $lon
                ]
            ]
        ];
        return $this;
    }


    /*************************************** 子查询 **************************************/
    /**
     * 设置Nested 子查询
     * @param string $path Nested字段路径
     * @param string $type 查询类型 (term, terms, range, match)
     * @param string $field 查询字段
     * @param mixed $value 查询值
     * @param array $options 额外选项
     *  暂时只有'type'判断是哪个类型的搜索，默认为must @todo...
     * @return $this
     * @example
     *  $esTools->setNestedQuery('goods', 'term', 'goods.price', 100);
     *  $esTools->setNestedQuery('goods', 'terms', 'goods.price_id', [1001, 1002]);
     *  $esTools->setNestedQuery('goods', 'range', 'goods.price', ['>=' => 10, '<' => 100]);
     *  $esTools->setNestedQuery('goods', 'match', 'goods.goods_name', '大鱼王 矶钓');
     * @author gengjianhao
     * @time 2023/09/22 15:38
     */
    public function setNestedQuery($path, $type, $field, $value, $options = ['type' => 'must']) {
        $query = [];
        $parentType = $options['type'] ?? 'must';

        switch ($type) {
            case 'term':
                $query = ['term' => [$field => $value]];
                break;
            case 'terms':
                // 注意：$value应该是一个数组
                $query = ['terms' => [$field => $value]];
                break;
            case 'range':
                $rangeOps = [];
                foreach ($value as $operator => $val) {
                    switch ($operator) {
                        case '>':
                            $rangeOps['gt'] = $val;
                            break;
                        case '>=':
                            $rangeOps['gte'] = $val;
                            break;
                        case '<':
                            $rangeOps['lt'] = $val;
                            break;
                        case '<=':
                            $rangeOps['lte'] = $val;
                            break;
                        default:
                            throw new Exception("Invalid range operator: $operator");
                    }
                }
                $query = ['range' => [$field => $rangeOps]];
                break;
            case 'match':
                $matchParams = $this->_handleMatch($field, $value);
                list($searchType, $searchParam) = array($matchParams['type'], $matchParams['params']);
                $query = [$searchType => $searchParam];;
                break;
        }

        $this->conditions['nested'][] = [
            'nested' => [
                'path' => $path,
                'query' => $query,
                'parent_type' => $parentType
            ]
        ];
        return $this;
    }


    /*************************************** 分页排序相关 **************************************/
    /**
     * 排序
     * @param $field
     * @param $order
     * @return $this
     * @author gengjianhao
     * @time 2023/09/21 11:44
     */
    public function orderBy($field, $order = 'asc') {
        $this->sortFields[] = [$field => ['order' => $order]];
        return $this;
    }

    /**
     * 地理位置排序
     * @param string $field 地理位置字段名
     * @param float $latitude 用户的纬度
     * @param float $longitude 用户的经度
     * @param string $order 排序顺序（例如：asc 或 desc）
     * @param string $unit 距离单位（例如：km、m、mi）
     * @return $this
     * @example
     *  $esTools->orderByGeoDistance('location', $latitude, $longitude, 'asc')
     * @author gengjianhao
     * @time 2023/09/20 14:56
     */
    public function orderByGeoDistance($field, $latitude, $longitude, $order = 'asc', $unit = 'm') {
        $this->sortFields[] = [
            '_geo_distance' => [
                $field => [
                    'lat' => $latitude,
                    'lon' => $longitude
                ],
                'order' => $order,
                'unit' => $unit,
                //一个字段多个距离的时候取最小的
                "mode"  => "min",
                // distance_type可选项
                // arc（默认）使用地球的真实形状（一个椭球体）来计算两点之间的距离。这是最精确的方式，但也是最慢的方式
                // plane 假设地球是一个完美的平面来计算距离。这种方法比 "arc" 快，但在大距离（例如大于2,000km）上可能不够准确
                // 应用程序需要在大距离上工作并且需要高精度，那么 "arc" 可能是最佳选择。但如果速度是主要考虑因素，而且你的应用程序主要工作在较小的区域内，那么 "plane" 可能是一个更好的选择
                "distance_type" => "arc"
            ]
        ];
        $this->isDistance = TRUE;
        return $this;
    }

    /**
     * 展示数量
     * @param int $size
     * @return $this
     * @author gengjianhao
     * @time 2023/09/23 16:57
     */
    public function limit($size) {
        $this->size = $size;
        return $this;
    }

    /**
     * 偏移量
     * @param int $from
     * @return $this
     * @author gengjianhao
     * @time 2023/09/23 16:57
     */
    public function offset($from) {
        $this->from = $from;
        return $this;
    }

    /**
     * 根据输入的查询字符串，获取相关文档
     *
     * @param string $input 输入的查询字符串
     * @param string $suggestField 使用completion suggester的字段
     * @param int $from 从第几条数据开始
     * @param int $size 返回多少条文档
     * @param array $sort 排序规则
     * @return array 返回的文档结果
     */
    public function getSuggestions($input, $suggestField, $from = 0, $size = 10, $sort = ['create_time' => ['order' => 'desc']]) {
        // 第一步: 使用completion suggester获取建议词条
        $suggestParams = [
            'index' => $this->index,
            'body' => [
                'suggest' => [
                    'text' => $input,
                    'completion' => [
                        'field' => $suggestField,
                        'size' => $size
                    ]
                ]
            ]
        ];

        $suggestions = $this->client->search($suggestParams);

        $suggestedTerms = [];
        foreach ($suggestions['suggest'] as $suggestResult) {
            foreach ($suggestResult['options'] as $option) {
                $suggestedTerms[] = $option['text'];
            }
        }

        if (empty($suggestedTerms)) {
            return [];
        }

        // 第二步: 使用得到的词条进行普通搜索
        $searchParams = [
            'index' => $this->index,
            'body' => [
                'from' => $from,
                'size' => $size,
                'query' => [
                    'terms' => [
                        $suggestField => $suggestedTerms
                    ]
                ],
                'sort' => $sort
            ]
        ];

        $docResults = $this->client->search($searchParams);

        return isset($docResults['hits']['hits']) ? $docResults['hits']['hits'] : [];
    }

    /**
     * 对指定字段进行求和统计
     * @param string $sumField 要求和的字段
     * @param array $query
     * @return float
     * @author gengjianhao
     * @time 2023/09/23 17:19
     */
    public function aggregateBySum($sumField, $query = []) {
        $params = [
            'index' => $this->index,
            'body' => [
                'query' => $query,
                'aggs' => [
                    'total_sum' => [
                        'sum' => [
                            'field' => $sumField
                        ]
                    ]
                ]
            ]
        ];

        $response = $this->client->search($params);
        return $response['aggregations']['total_sum']['value'];
    }

    /**
     * 根据字段进行桶分析
     * @param string $field 要进行分析的字段
     * @param array $query
     * @return array
     * @author gengjianhao
     * @time 2023/09/23 17:21
     */
    public function aggregateByTerms($field, $query = []) {
        $params = [
            'index' => $this->index,
            'body' => [
                'query' => $query,
                'aggs' => [
                    'group_by_field' => [
                        'terms' => [
                            'field' => $field
                        ]
                    ]
                ]
            ]
        ];

        $response = $this->client->search($params);
        return $response['aggregations']['group_by_field']['buckets'];
    }

    // 统计某件商品的复购人数
    public function countRebuyUsersByGoods($goodsId) {
        $params = [
            'index' => $this->index,
            'body' => [
                'query' => [
                    'nested' => [
                        'path' => 'goods',
                        'query' => [
                            'term' => ['goods.goods_id' => $goodsId]
                        ]
                    ]
                ],
                'aggs' => [
                    'goods_agg' => [
                        'nested' => [
                            'path' => 'goods'
                        ],
                        'aggs' => [
                            'specific_goods_agg' => [
                                'filter' => [
                                    'term' => ['goods.goods_id' => $goodsId]
                                ],
                                'aggs' => [
                                    'users_agg' => [
                                        'terms' => [
                                            'field' => 'user_id',
                                            'min_doc_count' => 2
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        $response = $this->client->search($params);
        return count($response['aggregations']['goods_agg']['specific_goods_agg']['users_agg']['buckets']);
    }

    // 统计某个商品的总人数
    public function countUsersByGoods($goodsId) {
        $params = [
            'index' => $this->index,
            'body' => [
                'query' => [
                    'nested' => [
                        'path' => 'goods',
                        'query' => [
                            'term' => ['goods.goods_id' => $goodsId]
                        ]
                    ]
                ],
                'aggs' => [
                    'goods_agg' => [
                        'nested' => [
                            'path' => 'goods'
                        ],
                        'aggs' => [
                            'specific_goods_agg' => [
                                'filter' => [
                                    'term' => ['goods.goods_id' => $goodsId]
                                ],
                                'aggs' => [
                                    'users_agg' => [
                                        'terms' => [
                                            'field' => 'user_id'
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ];

        $response = $this->client->search($params);
        return count($response['aggregations']['goods_agg']['specific_goods_agg']['users_agg']['buckets']);
    }


    /*************************************** 公共处理方法 **************************************/
    /**
     * 处理match语句
     * @params string $field
     * @params string $value
     * @return array
     * @author gengjianhao
     * @time 2023/09/22 15:42
     */
    private function _handleMatch($field, $value) {
        $type = 'match';
        $queryParams = array();

        if (strpos($value, ' ')) {
            // $queryParams['match'][$field] = $value;
            $queryParams[$field] = array(
                'query' => $value,
                'operator' => 'and'
            );
        } else {
            $type = 'match_phrase';
            $queryParams[$field] = $value;
        }

        return array('type' => $type, 'params' => $queryParams);
    }

}

    // 基础调用Demo
    // $esTools = elasticTools::getInstance();
    // $size = 20;
    // $pageSize = intval(g('page'));
    // $results = $esTools->setIndex('order')
    //    ->where('status', 2)
    //    ->whereIn('shopping_status', array(1, 2))
    //    ->match('consignee', '李大侠')
    //    ->orderBy('create_time', 'desc')
    //    ->limit($size)
    //    ->offset(($pageSize - 1) * $size)
    //    ->search();

    // 统计每个人的订单总额
    // $result = $esTools->aggregateBySum('order_total', ['term' => ['user_id' => 10442971]]);
    // echo "张三的订单总额是: " . $result;

?>
