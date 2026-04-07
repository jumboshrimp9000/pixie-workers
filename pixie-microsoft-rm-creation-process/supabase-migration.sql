-- Migration: Add interim_status and action_history to domains table
-- for step-level pipeline tracking and admin panel resume functionality.

-- interim_status tracks exactly where in the provisioning pipeline a domain is.
-- This enables the admin panel to show progress and allow "Continue from last step".
ALTER TABLE domains ADD COLUMN IF NOT EXISTS interim_status TEXT;
ALTER TABLE domains ADD COLUMN IF NOT EXISTS action_history TEXT;

-- Index for filtering by interim_status in admin panel
CREATE INDEX IF NOT EXISTS idx_domains_interim_status ON domains(interim_status) WHERE interim_status IS NOT NULL;
