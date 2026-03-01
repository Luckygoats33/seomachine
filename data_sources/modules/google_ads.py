"""
Google Ads Data Integration

Fetches campaign performance, keyword data, and search terms from Google Ads
using OAuth refresh tokens (Google Ads API doesn't support service accounts).
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


class GoogleAds:
    """Google Ads data fetcher for SEO content intelligence"""

    def __init__(
        self,
        customer_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
    ):
        """
        Initialize Google Ads client

        Args:
            customer_id: Google Ads customer ID (defaults to env var GOOGLE_ADS_CUSTOMER_ID)
            credentials_path: Path to token JSON file (defaults to env var GOOGLE_ADS_CREDENTIALS_PATH)
        """
        self.customer_id = (customer_id or os.getenv('GOOGLE_ADS_CUSTOMER_ID', '')).replace('-', '')
        credentials_path = credentials_path or os.getenv('GOOGLE_ADS_CREDENTIALS_PATH')

        if not self.customer_id:
            raise ValueError("GOOGLE_ADS_CUSTOMER_ID must be provided or set in environment")

        if not credentials_path or not os.path.exists(credentials_path):
            raise ValueError(f"Credentials file not found: {credentials_path}")

        with open(credentials_path, 'r') as f:
            creds = json.load(f)

        self.client = GoogleAdsClient.load_from_dict({
            'developer_token': creds['developer_token'],
            'client_id': creds['client_id'],
            'client_secret': creds['client_secret'],
            'refresh_token': creds['refresh_token'],
            'login_customer_id': creds.get('login_customer_id', ''),
            'use_proto_plus': True,
        })

        self.ga_service = self.client.get_service('GoogleAdsService')

    def _query(self, query: str) -> List[Any]:
        """Execute a GAQL query and return rows"""
        try:
            response = self.ga_service.search_stream(
                customer_id=self.customer_id,
                query=query,
            )
            rows = []
            for batch in response:
                for row in batch.results:
                    rows.append(row)
            return rows
        except GoogleAdsException as e:
            print(f"Google Ads API error: {e.failure.errors[0].message}")
            return []

    def _micros_to_dollars(self, micros: int) -> float:
        return micros / 1_000_000

    def _date_range_clause(self, days: int) -> str:
        if days == 7:
            return "segments.date DURING LAST_7_DAYS"
        elif days == 14:
            return "segments.date DURING LAST_14_DAYS"
        elif days == 30:
            return "segments.date DURING LAST_30_DAYS"
        elif days == 90:
            return "segments.date DURING LAST_90_DAYS"
        else:
            start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            end = datetime.now().strftime('%Y-%m-%d')
            return f"segments.date BETWEEN '{start}' AND '{end}'"

    def get_campaign_performance(
        self,
        days: int = 30,
    ) -> List[Dict[str, Any]]:
        """
        Get campaign-level performance metrics

        Args:
            days: Number of days to look back

        Returns:
            List of dicts with campaign performance data
        """
        query = f"""
            SELECT
                campaign.name,
                campaign.status,
                campaign.advertising_channel_type,
                metrics.cost_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value,
                metrics.cost_per_conversion
            FROM campaign
            WHERE {self._date_range_clause(days)}
                AND campaign.status != 'REMOVED'
            ORDER BY metrics.cost_micros DESC
        """

        rows = self._query(query)
        results = []
        for row in rows:
            cost = self._micros_to_dollars(row.metrics.cost_micros)
            conv_value = row.metrics.conversions_value
            roas = (conv_value / cost) if cost > 0 else 0

            results.append({
                'campaign': row.campaign.name,
                'status': row.campaign.status.name,
                'channel_type': row.campaign.advertising_channel_type.name,
                'cost': round(cost, 2),
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'ctr': round(row.metrics.ctr * 100, 2),
                'avg_cpc': round(self._micros_to_dollars(row.metrics.average_cpc), 2),
                'conversions': round(row.metrics.conversions, 1),
                'conversion_value': round(conv_value, 2),
                'cost_per_conversion': round(self._micros_to_dollars(row.metrics.cost_per_conversion), 2),
                'roas': round(roas, 2),
            })

        return results

    def get_keyword_performance(
        self,
        days: int = 30,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Get keyword-level performance with quality scores

        Args:
            days: Number of days to look back
            limit: Max keywords to return

        Returns:
            List of dicts with keyword metrics and quality score
        """
        query = f"""
            SELECT
                ad_group.name,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.quality_info.quality_score,
                metrics.cost_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.cost_per_conversion
            FROM keyword_view
            WHERE {self._date_range_clause(days)}
                AND metrics.impressions > 0
            ORDER BY metrics.cost_micros DESC
            LIMIT {limit}
        """

        rows = self._query(query)
        results = []
        for row in rows:
            results.append({
                'keyword': row.ad_group_criterion.keyword.text,
                'match_type': row.ad_group_criterion.keyword.match_type.name,
                'ad_group': row.ad_group.name,
                'quality_score': row.ad_group_criterion.quality_info.quality_score or None,
                'cost': round(self._micros_to_dollars(row.metrics.cost_micros), 2),
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'ctr': round(row.metrics.ctr * 100, 2),
                'avg_cpc': round(self._micros_to_dollars(row.metrics.average_cpc), 2),
                'conversions': round(row.metrics.conversions, 1),
                'cost_per_conversion': round(self._micros_to_dollars(row.metrics.cost_per_conversion), 2),
            })

        return results

    def get_search_terms(
        self,
        days: int = 30,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get actual search queries triggering ads — gold mine for SEO content ideas

        Args:
            days: Number of days to look back
            limit: Max search terms to return

        Returns:
            List of dicts with search term data
        """
        query = f"""
            SELECT
                search_term_view.search_term,
                search_term_view.status,
                campaign.name,
                ad_group.name,
                metrics.cost_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value
            FROM search_term_view
            WHERE {self._date_range_clause(days)}
                AND metrics.impressions > 0
            ORDER BY metrics.impressions DESC
            LIMIT {limit}
        """

        rows = self._query(query)
        results = []
        for row in rows:
            cost = self._micros_to_dollars(row.metrics.cost_micros)
            conv_value = row.metrics.conversions_value

            results.append({
                'search_term': row.search_term_view.search_term,
                'status': row.search_term_view.status.name,
                'campaign': row.campaign.name,
                'ad_group': row.ad_group.name,
                'cost': round(cost, 2),
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'ctr': round(row.metrics.ctr * 100, 2),
                'avg_cpc': round(self._micros_to_dollars(row.metrics.average_cpc), 2),
                'conversions': round(row.metrics.conversions, 1),
                'conversion_value': round(conv_value, 2),
            })

        return results

    def get_cost_by_keyword(
        self,
        days: int = 30,
        min_cost: float = 1.0,
    ) -> List[Dict[str, Any]]:
        """
        Get high-CPC keywords — these are high-value organic targets

        Keywords with high CPC indicate commercial intent and competition.
        Ranking organically for these saves significant ad spend.

        Args:
            days: Number of days to look back
            min_cost: Minimum total cost to include

        Returns:
            List of dicts sorted by avg CPC descending
        """
        query = f"""
            SELECT
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.quality_info.quality_score,
                metrics.cost_micros,
                metrics.impressions,
                metrics.clicks,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value
            FROM keyword_view
            WHERE {self._date_range_clause(days)}
                AND metrics.impressions > 0
            ORDER BY metrics.average_cpc DESC
            LIMIT 100
        """

        rows = self._query(query)
        results = []
        for row in rows:
            cost = self._micros_to_dollars(row.metrics.cost_micros)
            if cost < min_cost:
                continue

            avg_cpc = self._micros_to_dollars(row.metrics.average_cpc)
            conv_value = row.metrics.conversions_value
            conversions = row.metrics.conversions

            # Estimate monthly organic value: if you ranked #1 organically for this keyword,
            # how much ad spend would you save?
            estimated_monthly_value = round(avg_cpc * row.metrics.clicks, 2) if row.metrics.clicks else 0

            results.append({
                'keyword': row.ad_group_criterion.keyword.text,
                'match_type': row.ad_group_criterion.keyword.match_type.name,
                'quality_score': row.ad_group_criterion.quality_info.quality_score or None,
                'total_cost': round(cost, 2),
                'avg_cpc': round(avg_cpc, 2),
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'conversions': round(conversions, 1),
                'conversion_value': round(conv_value, 2),
                'estimated_monthly_organic_value': estimated_monthly_value,
            })

        # Sort by avg CPC descending
        results.sort(key=lambda x: x['avg_cpc'], reverse=True)
        return results


# Example usage
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv('data_sources/config/.env')

    gads = GoogleAds()

    print("Campaign Performance (30 days):")
    print("-" * 60)
    campaigns = gads.get_campaign_performance(days=30)
    for c in campaigns:
        print(f"  {c['campaign']} [{c['status']}]")
        print(f"    Cost: ${c['cost']:.2f} | Clicks: {c['clicks']:,} | Conv: {c['conversions']}")
        print(f"    ROAS: {c['roas']:.2f}x | CPC: ${c['avg_cpc']:.2f}")
        print()

    print("\nTop Search Terms (content ideas):")
    print("-" * 60)
    terms = gads.get_search_terms(days=30, limit=20)
    for t in terms:
        print(f"  \"{t['search_term']}\"")
        print(f"    Impr: {t['impressions']:,} | Clicks: {t['clicks']} | CPC: ${t['avg_cpc']:.2f}")
        print()

    print("\nHigh-CPC Keywords (organic targets):")
    print("-" * 60)
    expensive = gads.get_cost_by_keyword(days=30)
    for k in expensive[:10]:
        print(f"  \"{k['keyword']}\" — ${k['avg_cpc']:.2f}/click")
        print(f"    Monthly organic value: ${k['estimated_monthly_organic_value']:.2f}")
        print()
