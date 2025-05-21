CALL gds.graph.project(
  'ratingsGraph',
  ['User', 'Movie'],
  {
    RATED: {
      properties: 'rating'
    }
  }
);

CALL gds.pageRank.stream('ratingsGraph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).id AS id, labels(gds.util.asNode(nodeId)) AS labels, score;
