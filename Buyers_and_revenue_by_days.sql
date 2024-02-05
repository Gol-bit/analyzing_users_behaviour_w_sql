SELECT `day`,
       `userId`,
       `customers`,
       `revenue_R$`,
       `customers_total`,
       `revenue_R$_total`
FROM
  (SELECT toStartOfDay(toDateTime(`ts`)) AS `day`,
          CONCAT('creator-', tenantId) AS `userId`,
          uniqIf(userId, eventType='server_watcher_purchase_success'
                 and price>0) AS `customers`,
          round(sumIf(price, eventType='server_watcher_purchase_success'
                      and price>0), 1) AS `revenue_R$`,
          round(SUM(round(sumIf(price, eventType='server_watcher_purchase_success'
                                and price>0), 1)) OVER (PARTITION BY tenantId
                                                        ORDER BY toStartOfDay(toDateTime(`ts`)))) AS `revenue_R$_total`,
          SUM(uniqIf(userId, eventType='server_watcher_purchase_success'
                     and price>0)) OVER (PARTITION BY tenantId) AS `customers_total`
   FROM
     (SELECT userId,
             a.tenantId as tenantId,
             deviceId,
             ip,
             a.sessionId,
             toBrazilTs(ts) as ts,
             eventType,
             multiIf(platform = 'Web'
                     AND deviceModel IN ('Windows', 'Linux', 'Mac OS'), 'Desktop', platform = 'Web'
                     AND deviceModel NOT IN ('Windows', 'Linux', 'Mac OS'), 'Mobile Web', platform = 'iOS', 'App iOS', platform = 'Android', 'App Android', 'other/unknown') as app_type,
             videoId,
             toFloat32OrZero(price) as price,
             initial_utm_source as initial_utm_source_creator,
             initial_utm_campaign as initial_utm_campaign_creator,
             initial_utm_medium as initial_utm_medium_creator,
             initial_referrer as initial_referrer_creator,
             c.utmCampaign as utm_campaign_watcher,
             c.utmSource as utm_source_watcher,
             c.utmMedium as utm_medium_watcher,
             c.utmContent as utm_content_watcher,
             c.referrer as referrer_watcher
      FROM analytics.watcher a
      LEFT JOIN
        (select tenantId,
                initial_utm_source,
                initial_utm_campaign,
                initial_utm_medium,
                initial_referrer
         from work.creator_registrations) as b ON a.tenantId = b.tenantId
      LEFT JOIN
        (select utmCampaign,
                utmSource,
                utmMedium,
                utmContent,
                referrer,
                sessionId
         from work.tbl_device_session_utm) as c ON a.sessionId = c.sessionId
      WHERE ts::DATE >='2022-09-01'
        AND eventType in ('start_session',
                          'click_button_cookies_accept',
                          'click_button_cookies_reject',
                          'start_playback',
                          'select_placeholder',
                          'click_button_buy_on_placeholder',
                          'show_screen_registration',
                          'click_button_send_code',
                          'click_button_send_sms',
                          'show_screen_my_purchase',
                          'view_live',
                          'start_playback_live',
                          'complete_registration',
                          'login',
                          'init_purchase',
                          'complete_purchase',
                          'server_watcher_purchase_success')) AS `virtual_table`
   WHERE `ts` >= toDateTime('2022-09-01 15:11:33')
   GROUP BY tenantId,
            toStartOfDay(toDateTime(`ts`))
   HAVING ((uniqIf(deviceId, eventType='server_watcher_purchase_success') > 0))
   ORDER BY `day` DESC
   LIMIT 10000) AS `virtual_table`
LIMIT 1000;
