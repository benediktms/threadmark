CREATE INDEX events_effort_entity_time ON events(effort_id, entity_type, entity_id, occurred_at);
CREATE INDEX events_effort_actor_time ON events(effort_id, actor_id, occurred_at);
CREATE INDEX events_effort_type_time ON events(effort_id, event_type, occurred_at);
