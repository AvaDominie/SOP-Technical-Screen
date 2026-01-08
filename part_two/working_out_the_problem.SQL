/* I first need to understand the problem. I need to create a list of ReferringPractices that meet 
the following criteria: More than one Site referred to in the last 6 months and the sites they were referred to. */


/* DESIGN DECISION:
    Interpretation of "multiple SOP sites":
    - The requirement asks for practices that referred to "multiple SOP sites" and "which sites they referred them to"
    - I interpreted this to include ALL referrals, not just unique sites
    - Reasioning: Marketing wants to understand the "spread" of referrals, which includes:
    1. VOLUME: How many total referrals from each practice (including repeat referrals to same site)
    2. DISTRIBUTION: Which sites are being used (including duplicates to show frequency)
    - This approach gives Marketing a fuller picture of referral activity and patterns, showing both volume and distribution.


    This approach gives Marketing a fuller picture of referral activity and patterns,
    showing both which sites are used AND how frequently each practice refers to them.
*/


/* Step 1: Get referrals from the last 6 months */
WITH six_month_referrals AS (
    SELECT
        ReferringPractice,
        Site
    FROM Gold.patient_referrals
    WHERE EntryDate >= DATEADD(MONTH, -6, GETDATE())
),
/* Step 2: Count the number of distinct sites each practice referred to in the last 6 months and list the sites */
practice_site_counts AS (
    SELECT 
        ReferringPractice,
        COUNT(Site) AS NumberOfSites,
        STRING_AGG(Site, ', ') WITHIN GROUP (ORDER BY Site) AS SitesReferredTo
    FROM six_month_referrals
    GROUP BY ReferringPractice
)
/* Step 3: Select practices referring to multiple sites */
SELECT 
    ReferringPractice,
    NumberOfSites,
    SitesReferredTo
FROM practice_site_counts
WHERE NumberOfSites > 1  -- Only practices referring to multiple sites
ORDER BY NumberOfSites DESC, ReferringPractice;