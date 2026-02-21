from __future__ import annotations

from dagster import Definitions

from .jobs.clinical_trial_enrollment_sf import clinical_trial_enrollment_sf_job

defs = Definitions(jobs=[clinical_trial_enrollment_sf_job])
