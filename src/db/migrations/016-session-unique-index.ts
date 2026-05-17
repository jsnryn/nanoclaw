import type { Migration } from './index.js';

export const migration016: Migration = {
  version: 16,
  name: '016-session-unique-index',
  up: (db) => {
    // Deduplicate existing sessions: keep the oldest per (agent_group_id,
    // messaging_group_id, thread_id) triple and archive the rest.
    db.exec(`
      UPDATE sessions SET status = 'archived'
      WHERE id NOT IN (
        SELECT MIN(id) FROM sessions
        WHERE status = 'active'
        GROUP BY agent_group_id, COALESCE(messaging_group_id, ''), COALESCE(thread_id, '')
      )
      AND status = 'active'
      AND messaging_group_id IS NOT NULL;

      CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_unique_active
        ON sessions(agent_group_id, messaging_group_id, thread_id)
        WHERE status = 'active' AND messaging_group_id IS NOT NULL;
    `);
  },
};
