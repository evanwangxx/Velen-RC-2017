SELECT 
ROUND((100.0 * (SUM(CASE WHEN 
              risklevel IN (5, 6, 23, 36, 53, 40, 4, 18, 31, 27, 33, 34, 35)
              AND dt = 20170607 
              AND (action = 32 OR action = 33)
             THEN 1    
         ELSE 0
         END) / SUM( CASE WHEN dt = 20170607 
                    AND (action = 32 OR action = 33) THEN 1
                   ELSE 0
                   END)
         )
), 3) AS D0607,   

 ROUND((100.0 * (SUM(CASE WHEN 
              risklevel IN (5, 6, 23, 36, 53, 40, 4, 18, 31, 27, 33, 34, 35)
              AND dt = 20170608 
              AND (action = 32 OR action = 33)
             THEN 1    
         ELSE 0
         END) / SUM( CASE WHEN dt = 20170608 
                    AND (action = 32 OR action = 33) THEN 1
                   ELSE 0
                   END)
          )
 ),3) AS D0608, 
 
  ROUND((100.0 * (SUM(CASE WHEN 
              risklevel IN (5, 6, 23, 36, 53, 40, 4, 18, 31, 27, 33, 34, 35)
              AND dt = 20170609 
              AND (action = 32 OR action = 33)
             THEN 1    
         ELSE 0
         END) / SUM( CASE WHEN dt = 20170609 
                    AND (action = 32 OR action = 33) THEN 1
                   ELSE 0
                   END)
           )
  ),3) AS D0609,  
         
   ROUND((100.0 * (SUM(CASE WHEN 
              risklevel IN (5, 6, 23, 36, 53, 40, 4, 18, 31, 27, 33, 34, 35)
              AND dt = 20170610 
              AND (action = 32 OR action = 33)
             THEN 1    
         ELSE 0
         END) / SUM( CASE WHEN dt = 20170610 
                    AND (action = 32 OR action = 33) THEN 1
                   ELSE 0
                   END)
            )
   ),3) AS D0610,
         
   ROUND((100.0 * (SUM(CASE WHEN 
              risklevel IN (5, 6, 23, 36, 53, 40, 4, 18, 31, 27, 33, 34, 35)
              AND dt = 20170611 
              AND (action = 32 OR action = 33)
             THEN 1    
         ELSE 0
         END) / SUM( CASE WHEN dt = 20170611 
                    AND (action = 32 OR action = 33) THEN 1
                   ELSE 0
                   END)
               )
      ),3) AS D0611        
FROM ba_rc.web_rc_risklog 
WHERE dt IN (20170607,20170608,20170609,20170610,20170611);