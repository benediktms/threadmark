CREATE TRIGGER prevent_reverse_contradiction
BEFORE INSERT ON edges
WHEN NEW.type = 'contradicts'
  AND EXISTS (
    SELECT 1
    FROM edges
    WHERE effort_id = NEW.effort_id
      AND type = NEW.type
      AND source_node_id = NEW.target_node_id
      AND target_node_id = NEW.source_node_id
  )
BEGIN
  SELECT RAISE(ABORT, 'duplicate symmetric contradiction edge');
END;
